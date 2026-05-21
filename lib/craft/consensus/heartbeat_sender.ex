defmodule Craft.Consensus.HeartbeatSender do
  use GenServer

  require Logger
  import Craft.Consensus, only: [heartbeat_interval: 0]
  alias Craft.Consensus

  defmodule State do
    defstruct [:cur_tick_ref, :timeout_ref, num_waiting: 0, heartbeat_buffer: %{}]
  end

  def send_follower_responses(node, responses) do
    GenServer.cast({__MODULE__, node}, {:follower_heartbeat_responses, responses})
  end

  def consensus_tick_response(tick_ref, group_name, response) do
    GenServer.cast(__MODULE__, {:consensus_tick_response, tick_ref, group_name, response})
  end

  def send_now(group, messages) do
    GenServer.cast(__MODULE__, {:send_now, group, messages})
  end

  def start_link(_) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def init([]) do
    send(self(), :tick)

    {:ok, %State{}}
  end

  def handle_info(:tick, state) do
    Process.send_after(self(), :tick, heartbeat_interval())

    tick_ref = make_ref()

    groups = Craft.Application.running_groups()
    
    for group <- groups do
      :gen_statem.cast(Craft.Application.lookup(group, Craft.Consensus), {:tick, tick_ref})
    end

    timeout_ref = Process.send_after(self(), :tick_response_timeout, 25)

    {:noreply, %{state | cur_tick_ref: tick_ref, timeout_ref: timeout_ref, num_waiting: Enum.count(groups)}}
  end

  def handle_info(:tick_response_timeout, state) do
    flush(state.heartbeat_buffer)

    {:noreply, %State{}}
  end

  def handle_cast({:send_now, group, messages}, state) do
    state.heartbeat_buffer
    |> update_heartbeat_buffer(group, messages)
    |> flush()

    {:noreply, %{state | heartbeat_buffer: %{}}}
  end

  def handle_cast({:consensus_tick_response, tick_ref, _group, _messages}, state)
      when tick_ref != state.cur_tick_ref do
    Logger.warning("HeartbeatSender received expired tick response from consensus")

    {:noreply, state}
  end

  def handle_cast({:consensus_tick_response, _tick_ref, group, messages}, state)
      when state.num_waiting <= 1 do
    state.heartbeat_buffer
    |> update_heartbeat_buffer(group, messages)
    |> flush()

    Process.cancel_timer(state.timeout_ref)

    {:noreply, %State{}}
  end

  def handle_cast({:consensus_tick_response, _tick_ref, group, messages}, state) do
    state =
      %{
        state
        | heartbeat_buffer: update_heartbeat_buffer(state.heartbeat_buffer, group, messages),
          num_waiting: state.num_waiting - 1
      }

    {:noreply, state}
  end

  def handle_cast({:follower_heartbeat_responses, responses}, state) do
    for {group, message_responses} <- responses do
      for message_response <- message_responses do
        Consensus.do_operation(:cast, group, message_response)
      end
    end

    {:noreply, state}
  end

  defp flush(heartbeats) do
    for {node, heartbeats} <- heartbeats do
      Craft.Consensus.HeartbeatReceiver.distribute(node, node(), heartbeats)
    end
  end

  defp update_heartbeat_buffer(buffer, _group, :not_leader), do: buffer

  defp update_heartbeat_buffer(buffer, group, messages) do
    Enum.reduce(messages, buffer, fn {dest, message}, acc ->
      Map.update(acc, dest, %{group => [message]}, fn by_group ->
        Map.update(by_group, group, [message], &(&1 ++ [message]))
      end)
    end)
  end
end
