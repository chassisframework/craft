defmodule Craft.SimpleMachine do
  use Craft.Machine, mutable: false

  alias Craft.Linearizability.TestModel

  @behaviour TestModel

  def put(name, k, v, opts \\ []) do
    Craft.command({:put, k, v}, name, opts)
  end

  def get(name, k, opts \\ []) do
    Craft.query({:get, k}, name, opts)
  end

  @impl TestModel
  def init, do: init(nil)

  @impl true
  def init(_group_name) do
    {:ok, %{}}
  end

  @impl TestModel
  def write(command, state), do: handle_command(command, nil, state)

  @impl TestModel
  def read(query, state) do
    id = {self(), make_ref()}
    {:reply, resp} = handle_query(query, id, state)
    resp
  end

  @impl true
  def handle_command({:put, k, v}, _log_index, state) do
    {:ok, Map.put(state, k, v)}
  end

  @impl true
  def handle_query({:get, k}, _from, state) do
    {:reply, {:ok, Map.get(state, k)}}
  end

  @impl true
  def handle_role_change(new_role, state) do
    case Map.fetch(state, :test_process_from) do
      {:ok, {pid, ref}} ->
        send(pid, {ref, {:role_change, node(), new_role}})

      _ ->
        :noop
    end

    state
  end

  @impl true
  def handle_info({from, message}, state) do
    send(from, message)

    state
  end

  @impl true
  def receive_snapshot(snapshot, _state) do
    snapshot
  end

  @impl true
  def snapshot(state) do
    state
  end

  def dump(state), do: state
end
