defmodule Craft.Tracing do
  alias Craft.Consensus.State, as: ConsensusState

  def logger_metadata(extras) when is_list(extras) do
    # :logger uses the :time keyword (in microseconds), we want nanoseconds

    Keyword.merge([t: :os.system_time(:nanosecond)], extras)
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
end
