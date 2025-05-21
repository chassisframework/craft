defmodule Craft.SimpleMachine do
  use Craft.Machine, mutable: false

  def put(name, k, v, opts \\ []) do
    Craft.command({:put, k, v}, name, opts)
  end

  def get(name, k, opts \\ []) do
    Craft.query({:get, k}, name, opts)
  end

  @impl true
  def init(_group_name) do
    {:ok, %{}}
  end

  @impl true
  def handle_command({:put, k, v}, _log_index, state) do
    {:ok, Map.put(state, k, v)}
  end

  @impl true
  def handle_query({:get, k}, state) do
    {:ok, Map.get(state, k)}
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
