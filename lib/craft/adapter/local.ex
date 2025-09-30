defmodule Craft.Adapter.Local do
  @moduledoc """
  This module is useful for local development so that apps can be run
  on a single non-distributed node without having all the quorum and HA guarentees. 

  To use, put craft behind an interface and swap to using this module instead of the
  Craft module.

  Be sure to add this to your application children i.e.

  ```
  children =
    [
      {Store.Local, disabled?: !Application.get_env(:myapp, use_local_store?, false)},
      ...
    ]
  ```
  """

  use GenServer

  def discover(_group, _nodes), do: :noop
  def transfer_leadership(_group, _to_node), do: :noop
  def add_member(_group, _nodes), do: :noop
  def holding_lease?(nil), do: false
  def holding_lease?(_), do: true

  def cached_info(nil), do: :not_found
  def cached_info(_group), do: {:ok, %{leader: node()}}

  def start_member(group, inst \\ __MODULE__) do
    GenServer.call(inst, {:start_member, group})
  end

  def start_group(group, _nodes, machine, _opts \\ [], machine_opts \\ [], inst \\ __MODULE__) do
    GenServer.call(inst, {:start_group, group, machine, machine_opts})
  end

  def send(group, message, inst \\ __MODULE__) do
    GenServer.cast(inst, {:user_message, group, message})
  end

  def command(command, group, _opts, inst \\ __MODULE__) do
    GenServer.call(inst, {:command, group, command})
  end

  def query(query, group, _opts, inst \\ __MODULE__) do
    GenServer.call(inst, {:query, group, query})
  end

  def start_link(opts \\ []) do
    gen_server_opts = if name = Keyword.get(opts, :name, __MODULE__), do: [name: name], else: []
    GenServer.start_link(__MODULE__, opts, gen_server_opts)
  end

  def start(opts \\ []) do
    gen_server_opts = if name = Keyword.get(opts, :name, __MODULE__), do: [name: name], else: []
    GenServer.start(__MODULE__, opts, gen_server_opts)
  end

  @impl GenServer
  def init(opts) do
    if Keyword.get(opts, :disabled?, false) do
      :ignore
    else
      {:ok, %{groups: %{}}}
    end
  end

  @impl GenServer
  def handle_call({:start_member, group}, _from, state) do
    case Craft.Configuration.find(group) do
      %{machine: machine} ->
        {:ok, machine_state} = machine.init(group)
        state = put_in(state, [:groups, group], %{machine: machine, machine_state: machine_state})
        {:reply, :ok, state}

      _error ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl GenServer
  def handle_call({:start_group, group, machine, machine_opts}, _from, state) do
    if :in_memory not in machine_opts do
      config = %{machine: machine}
      Craft.Configuration.write_new!(group, config)
    end

    {:ok, machine_state} = machine.init(group, machine_opts)
    state = put_in(state, [:groups, group], %{machine: machine, machine_state: machine_state})
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_call({:command, group, command}, _from, state) do
    %{machine: machine, machine_state: machine_state} = Map.get(state.groups, group)
    {response, machine_state} = machine.handle_command(command, -1, machine_state)
    state = put_in(state, [:groups, group, :machine_state], machine_state)

    {:reply, response, state}
  end

  @impl GenServer
  def handle_call({:query, group, query}, from, state) do
    %{machine: machine, machine_state: machine_state} = Map.get(state.groups, group)

    case machine.handle_query(query, {:direct, from}, machine_state) do
      {:reply, response} -> {:reply, response, state}
      :noreply -> {:noreply, state}
    end
  end

  def handle_cast({:user_message, group, message}, state) do
    %{machine: machine, machine_state: machine_state} = Map.get(state.groups, group)
    machine_state = machine.handle_info(message, machine_state)

    {:noreply, put_in(state, [:groups, group, :machine_state], machine_state)}
  end

  @impl GenServer
  def handle_cast({{:query_reply, reply}, from}, state) do
    GenServer.reply(from, reply)
    {:noreply, state}
  end
end
