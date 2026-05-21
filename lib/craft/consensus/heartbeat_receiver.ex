defmodule Craft.Consensus.HeartbeatReceiver do
  use GenServer

  alias Craft.Consensus

  defmodule State do
    defstruct [
      # pending: %{{to_node, batch_id} => %{expected_groups: MapSet,
      #                                     responses: %{group => [msgs]},
      #                                     timer_ref: ref}}
      pending: %{}
    ]
  end

  # Public API

  def distribute(on_node, from_node, heartbeats) do
    GenServer.cast({__MODULE__, on_node}, {:distribute, from_node, heartbeats})
  end

  def add_response(to_node, batch_id, group_name, response) do
    GenServer.cast(__MODULE__, {:add_response, to_node, batch_id, group_name, response})
  end

  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  # GenServer Callbacks

  def init([]) do
    {:ok, %State{}}
  end

  def handle_cast({:distribute, from_node, heartbeats}, state) do
    batch_id = make_ref()

    for {group_name, messages} <- heartbeats do
      for message <- messages do
        message_with_batch_id = %{message | batch_id: batch_id}
        Consensus.do_operation(:cast, group_name, message_with_batch_id)
      end
    end

    expected_groups = MapSet.new(Map.keys(heartbeats))
    timer_ref = Process.send_after(self(), {:batch_timeout, from_node, batch_id}, 25)

    pending_entry = %{
      expected_groups: expected_groups,
      responses: %{},
      timer_ref: timer_ref,
      start_time: System.monotonic_time(:millisecond)
    }

    state = %{state | pending: Map.put(state.pending, {from_node, batch_id}, pending_entry)}

    {:noreply, state}
  end

  def handle_cast({:add_response, to_node, batch_id, group_name, response}, state) do
    case Map.get(state.pending, {to_node, batch_id}) do
      nil ->
        # Batch not found - already flushed or timed out
        {:noreply, state}

      pending_entry ->
        # IO.inspect(System.monotonic_time(:millisecond) - pending_entry.start_time, label: "Consensus response time")
        responses = Map.update(pending_entry.responses, group_name, [response], & &1 ++ [response])
        pending_entry = %{pending_entry | responses: responses}

        if map_size(responses) >= MapSet.size(pending_entry.expected_groups) do
          # Batch complete - flush and remove
          Process.cancel_timer(pending_entry.timer_ref)
          flush_responses(to_node, responses)
          state = %{state | pending: Map.delete(state.pending, {to_node, batch_id})}
          {:noreply, state}
        else
          # Still waiting for more responses
          state = %{state | pending: Map.put(state.pending, {to_node, batch_id}, pending_entry)}
          {:noreply, state}
        end
    end
  end

  def handle_info({:batch_timeout, to_node, batch_id}, state) do
    case Map.get(state.pending, {to_node, batch_id}) do
      nil ->
        # Batch already flushed (all responses received) (time should have been cancelled)
        {:noreply, state}

      pending_entry ->
        if map_size(pending_entry.responses) > 0 do
          flush_responses(to_node, pending_entry.responses)
        end

        state = %{state | pending: Map.delete(state.pending, {to_node, batch_id})}
        {:noreply, state}
    end
  end

  defp flush_responses(to_node, responses) do
    Consensus.HeartbeatSender.send_follower_responses(to_node, responses)
  end
end
