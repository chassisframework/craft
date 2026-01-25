defmodule Craft.MemberCache do
  @moduledoc false

  # The goal of the cache is to provide a fairly up-to-date view of a group's status: leadership, lease-holdership, membership, log_indexes, etc...
  # It most defiitely is a cache, though, no correctness guarantees.
  #
  # cache instances on followers and clients pull from the leader on an interval. followers patch in the current leader from raft messages, as well
  # as their own maximum log index.
  #

  use GenServer

  require Record

  alias Craft.Consensus.State, as: ConsensusState
  alias Craft.Consensus.State.Members
  alias Craft.GlobalTimestamp
  alias Craft.Persistence

  require Logger

  defmodule GroupStatus do
    @moduledoc false
    defstruct [:group_name, :lease_holder, :leader, :leader_ready, :members, :commit_index, :current_term]
  end
  Record.defrecord(:group_status, [:lease_holder, :leader, :leader_ready, :members, :commit_index, :current_term])
  defmacrop index(field), do: (quote do group_status(unquote(field)) + 1 end)

  @doc false
  def discover(group_name, members) do
    # initial bootstrap
    tuple =
      group_status(members: Map.new(members, fn member -> {member, %{}} end))
      |> put_elem(0, group_name)

    :ets.insert(__MODULE__, tuple)

    do_poll(group_name, members)

    :ok
  end

  @doc false
  def all do
    :ets.foldr(& [&1 | &2], [], __MODULE__)
    |> Map.new(fn record -> {elem(record, 0), new(record)} end)
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
  def holding_lease?(group_name) do
    with {lease_holder, global_clock, lease_expires_at} when lease_holder == node() <- :ets.lookup_element(__MODULE__, group_name, index(:lease_holder)),
         {:ok, time} when time > 0 <- GlobalTimestamp.time_until_lease_expires(global_clock, lease_expires_at) do
      true
    else
      _ ->
        false
    end
  end

  # indicates if the leader's machine has committed its term's section 5.4.2 entry (info only available on the leader)
  @doc false
  def leader_ready?(group_name) do
    :ets.lookup_element(__MODULE__, group_name, index(:leader)) == node() and :ets.lookup_element(__MODULE__, group_name, index(:leader_ready))
  end

  @doc false
  # follower
  def remote_update(%GroupStatus{members: members} = group_status) when is_map_key(members, node()) do
    local_members = :ets.lookup_element(__MODULE__, group_status.group_name, index(:members))
    members = put_in(members[node()].log_index, local_members[node()].log_index)

    elements = [
      {index(:leader_ready), group_status.leader_ready},
      {index(:lease_holder), group_status.lease_holder},
      {index(:commit_index), group_status.commit_index},
      {index(:current_term), group_status.current_term},
      {index(:members), members}
    ]

    true = :ets.update_element(__MODULE__, group_status.group_name, elements)
  end

  # client
  def remote_update(%GroupStatus{} = group_status) do
    elements = [
      {index(:leader_ready), group_status.leader_ready},
      {index(:lease_holder), group_status.lease_holder},
      {index(:commit_index), group_status.commit_index},
      {index(:current_term), group_status.current_term},
      {index(:leader), group_status.leader},
      {index(:members), group_status.members}
    ]

    true = :ets.update_element(__MODULE__, group_status.group_name, elements)
  end

  @doc false
  def non_leader_update(%ConsensusState{} = state) do
    if :ets.update_element(__MODULE__, state.name, {index(:leader), state.leader_id}) do
      latest_index = Persistence.latest_index(state.persistence)

      members = :ets.lookup_element(__MODULE__, state.name, index(:members))
      members =
        if members[node()] do
          put_in(members[node()].log_index, latest_index)
        else
          %{node() => %{log_index: latest_index}}
        end

      :ets.update_element(__MODULE__, state.name, {index(:members), members})

      :ok
    else
      discover(state.name, [])

      non_leader_update(state)
    end
  end

  @doc false
  def leader_update(%ConsensusState{} = state) do
    members =
      state.members
      |> Members.all_nodes()
      |> Map.new(fn member ->
        status =
          cond do
            member in state.members.voting_nodes ->
              :voting

            member in state.members.non_voting_nodes ->
              :non_voting

            member in state.members.catching_up_nodes ->
              :catching_up
          end

        log_index =
          if state.leader_state do
            if member == state.leader_id do
              Persistence.latest_index(state.persistence)
            else
              state.leader_state.match_indices[member]
            end
          end

        {member, %{status: status, log_index: log_index}}
      end)

    elements = [
      {index(:leader), state.leader_id},
      {index(:members), members},
      {index(:commit_index), state.commit_index},
      {index(:current_term), state.current_term}
    ]

    if :ets.update_element(__MODULE__, state.name, elements) do
      :ok
    else
      discover(state.name, [])

      leader_update(state)
    end
  end

  @doc false
  def update_lease_holder(%ConsensusState{} = state) do
    :ets.update_element(__MODULE__, state.name, {index(:lease_holder), {node(), state.global_clock, state.lease_expires_at}})
  end

  def set_leader_ready(group_name, leader_ready) do
    :ets.update_element(__MODULE__, group_name, {index(:leader_ready), leader_ready})
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
  def update_commit_index(group_name, new_members) do
    :ets.update_element(__MODULE__, group_name, {index(:members), new_members})
  end

  @doc false
  def delete(group_name) do
    :ets.delete(__MODULE__, group_name)
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
  def handle_info(:poll, state) do
    for {group_name, group_status} <- all() do
      followers =
        group_status.members
        |> Map.delete(group_status.leader)
        |> Map.keys()

      nodes =
        if group_status.leader do
          [group_status.leader | followers]
        else
          followers
        end

      if group_status.leader != node() do
        do_poll(group_name, nodes)
      end
    end

    Process.send_after(self(), :poll, 5_000)

    {:noreply, state}
  end

  defp do_poll(group_name, []) do
    Logger.warning("[MemberCache] can't get any info for group #{inspect group_name}")
  end

  defp do_poll(group_name, [node | rest]) when node == node(), do: do_poll(group_name, rest)

  defp do_poll(group_name, [node | rest]) do
    case :rpc.call(node, __MODULE__, :get, [group_name]) do
      {:ok, group_status} ->
        remote_update(group_status)

      _ ->
        do_poll(group_name, rest)
    end
  end

  defp new(record) do
    %GroupStatus{
      group_name: elem(record, 0),
      members: group_status(record, :members),
      leader: group_status(record, :leader),
      leader_ready: group_status(record, :leader_ready),
      lease_holder: group_status(record, :lease_holder),
      commit_index: group_status(record, :commit_index),
      current_term: group_status(record, :current_term)
    }
  end
end
