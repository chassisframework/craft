defmodule Craft.HeartbeatReceiver do
  use GenServer

  alias Craft.Consensus

  def distribute(node, heartbeats) do
    GenServer.cast({__MODULE__, node}, {:distribute, heartbeats})
  end

  def start_link([]) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init([]) do
    {:ok, nil}
  end

  def handle_cast({:distribute, heartbeats}, state) do
    for {name, msg} <- heartbeats do
      Consensus.do_operation(:cast, name, msg)
    end

    {:noreply, state}
  end
end
