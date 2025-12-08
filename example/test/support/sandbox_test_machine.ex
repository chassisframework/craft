defmodule Craft.SandboxTestMachine do
  use Craft.Machine, mutable: false

  @impl true
  def init(args) do
    {:ok, %{name: args.name}}
  end

  @impl true
  def handle_commands(commands, state) do
    {state, replies} = 
      Enum.reduce(commands, {state, []}, fn
        {{:put, k, v}, _index}, {state, replies} ->
          {Map.put(state, k, v), [:ok | replies]}
      end)

    {replies, state}
  end

  @impl true
  def handle_query({:get, k}, _from, state) do
    {:reply, {:ok, Map.get(state, k)}}
  end

  @impl true
  def handle_query({:get_parallel, k}, from, state) do
    spawn(fn ->
      try do
        Craft.reply(from, {:ok, Map.get(state, k)})
      rescue
        e ->
          {:direct, from, _sandbox_pid} = from
          GenServer.reply(from, {:error, e})
      end
    end)

    :noreply
  end

  @impl true
  def handle_info({_from, _message}, state) do
    state
  end

  @impl true
  def snapshot(_state), do: :noop

  @impl true
  def receive_snapshot(_snapshot, _state), do: :noop
end
