defmodule Craft.Consensus.State.LeaderState.CongestionControl do
  # multiplicative downregulation, additive upregulation cuts message sizes down quickly and slowly increases them as follower recovers
  alias Craft.Consensus
  alias Craft.Consensus.State

  import Craft.Tracing, only: [logger_metadata: 2]

  require Logger

  defstruct [entries_per_message: %{}]

  def new do
    %__MODULE__{}
  end

  def follower_lagging(%State{} = state, follower) do
    num_entries = state.leader_state.congestion_control.entries_per_message[follower] || Consensus.maximum_entries_per_heartbeat()
    num_entries = max(div(num_entries, 2), 1)

    if num_entries(state, follower) > num_entries do
      Logger.info("downregulating follower #{inspect follower} to #{num_entries}/msg", logger_metadata(state, trace: {:congestion_control, :downregulating, follower}))
    end

    put_in(state.leader_state.congestion_control.entries_per_message[follower], num_entries)
  end

  def follower_ok(%State{} = state, follower) do
    max = Consensus.maximum_entries_per_heartbeat()
    num_entries = state.leader_state.congestion_control.entries_per_message[follower] || max
    step = div(max, 10)
    num_entries = min(num_entries + step, max)

    if num_entries(state, follower) < max and num_entries == max do
      Logger.info("follower #{inspect follower} returned to unregulated flow (#{num_entries}/msg)", logger_metadata(state, trace: {:congestion_control, :upregulating, follower}))
    end

    put_in(state.leader_state.congestion_control.entries_per_message[follower], num_entries)
  end

  def num_entries(%State{} = state, follower) do
    state.leader_state.congestion_control.entries_per_message[follower] || Consensus.maximum_entries_per_heartbeat()
  end
end
