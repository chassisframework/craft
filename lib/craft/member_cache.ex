defmodule Craft.MemberCache do
  @moduledoc false
  use GenServer

  require Record

  alias Craft.Consensus.State, as: ConsensusState
  alias Craft.Consensus.State.Members
  alias Craft.Machine.State, as: MachineState
  alias Craft.GlobalTimestamp

  defmodule GroupStatus do
    @moduledoc false
    defstruct [:group_name, :lease_holder, :leader, :members]
  end
  Record.defrecord(:group_status, [:lease_holder, :leader, :members])
  defmacrop index(field), do: (quote do group_status(unquote(field)) + 1 end)

  @doc false
  def discover(group_name, members) do
    tuple =
      group_status(members: MapSet.new(members))
      |> put_elem(0, group_name)

    :ets.insert(__MODULE__, tuple)

    :ok
  end

  @doc false
  def all do
    :ets.foldr(& [&1 | &2], [], __MODULE__)
    |> Map.new(fn record -> {elem(record, 0), new(record)} end)
  end

  @doc false
  def holding_lease?(group_name) do
    with {lease_holder, global_clock, lease_expires_at} when lease_holder == node() <- :ets.lookup_element(__MODULE__, group_name, index(:lease_holder)),
         {:ok, time} when time > 0 <- GlobalTimestamp.time_until_lease_expires(global_clock, lease_expires_at) do
      true

    else
      _ ->
        false
    end
  end

  @doc false
  def update(%ConsensusState{} = state) do
    elements = [
      {index(:leader), state.leader_id},
      {index(:members), Members.all_nodes(state.members)}
    ]

    if :ets.update_element(__MODULE__, state.name, elements) do
      :ok
    else
      discover(state.name, [])
      update(state)
    end
  end

  def update(%GroupStatus{} = group_status) do
    elements = [
      {index(:leader), group_status.leader},
      {index(:members), group_status.members}
    ]

    true = :ets.update_element(__MODULE__, group_status.group_name, elements)
  end

  @doc false
  def update_lease_holder(%MachineState{} = state) do
    :ets.update_element(__MODULE__, state.name, {index(:lease_holder), {node(), state.global_clock, state.lease_expires_at}})
  end

  @doc false
  def update_leader(group_name, new_leader) do
    :ets.update_element(__MODULE__, group_name, {index(:leader), new_leader})
  end

  @doc false
  def update_members(group_name, new_members) do
    :ets.update_element(__MODULE__, group_name, {index(:members), new_members})
  end

  @doc false
  def get(group_name) do
    case :ets.lookup(__MODULE__, group_name) do
      [tuple] ->
        {:ok, new(tuple)}

      [] ->
        :not_found
    end
  end

  @doc false
  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  @impl GenServer
  def init(_args) do
    :ets.new(__MODULE__, [:set, :named_table, :public, read_concurrency: true])

    send(self(), :poll)

    {:ok, nil}
  end

  @impl GenServer
  def handle_call({:get, group_name}, _from, state) do
    {:reply, get(group_name), state}
  end

  @impl GenServer
  def handle_info(:poll, state) do
    for {group_name, group_status} <- all() do
      # group members should not poll their own group
      if not MapSet.member?(group_status.members, node()) do
        followers =
          group_status.members
          |> MapSet.delete(group_status.leader)
          |> MapSet.to_list()

        nodes = [group_status.leader | followers]

        do_poll(group_name, nodes)
      end
    end

    Process.send_after(self(), :poll, 5_000)

    {:noreply, state}
  end

  defp do_poll(_group_name, []), do: :not_found
  defp do_poll(group_name, [nil | rest]), do: do_poll(group_name, rest)
  defp do_poll(group_name, [node | rest]) do
    try do
      case GenServer.call({__MODULE__, node}, {:get, group_name}) do
        {:ok, group_status} ->
          update(group_status)

        :not_found ->
          do_poll(group_name, rest)
      end
    catch :exit, _e ->
      do_poll(group_name, rest)
    end
  end

  defp new(record) do
    %GroupStatus{
      group_name: elem(record, 0),
      members: group_status(record, :members),
      leader: group_status(record, :leader),
      lease_holder: group_status(record, :lease_holder)
    }
  end
end
