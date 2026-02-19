defmodule Craft.HeartbeatSender do
  use GenServer

  alias Craft.Message.AppendEntries
  alias Craft.HeartbeatReceiver

  import Craft.Consensus, only: [heartbeat_interval: 0]

  def enqueue(%AppendEntries{} = append_entries, to_node, name) do
    GenServer.cast(__MODULE__, {:enqueue, append_entries, to_node, name})
  end

  def start_link([]) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  defmodule State do
    defstruct [
      heartbeats_by_destination: %{}
    ]
  end

  def init([]) do
    send(self(), :send)

    {:ok, %State{}}
  end

  def handle_cast({:enqueue, append_entries, to_node, name}, state) do
    # not a Map.update, to avoid construction of the default `MapSet.new([{name, append_entries}])` argument with every call
    heartbeats = (state.heartbeats_by_destination[to_node] || MapSet.new()) |> MapSet.put({name, append_entries})
    state = put_in(state.heartbeats_by_destination[to_node], heartbeats)

    {:noreply, state}
  end

  def handle_info(:send, state) do
    Process.send_after(self(), :send, heartbeat_interval())

    for {node, heartbeats} <- state.heartbeats_by_destination do
      HeartbeatReceiver.distribute(node, heartbeats)
    end

    {:noreply, %State{}}
  end
end
