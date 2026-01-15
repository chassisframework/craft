defmodule Craft.Tracing do
  alias Craft.Consensus.State, as: ConsensusState
  alias Craft.GlobalTimestamp.NativeClock

  def logger_metadata(extras) when is_list(extras) do
    # :logger uses the :time keyword (in microseconds), we want nanoseconds

    Keyword.merge([t: NativeClock.monotonic_raw_time()], extras)
  end

  def logger_metadata(%ConsensusState{} = state, extras \\ []) do
    color =
      case state.state do
        :lonely ->
          :light_red

        :receiving_snapshot ->
          :magenta

        :follower ->
          :cyan

        :candidate ->
          :blue

        :leader ->
          :green
      end

    Keyword.merge([term: state.current_term, ansi_color: color], logger_metadata(extras))
  end

  def time(fun, metric, meta, measurements \\ %{}) do
    {duration_ms, return} = :timer.tc(fun, :millisecond)

    :telemetry.execute(metric, Map.merge(measurements, %{duration_ms: duration_ms}), meta)

    return
  end
end
