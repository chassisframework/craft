defmodule Craft.Consensus.State.LeaderState.QuorumStatus do
  alias Craft.Consensus.State
  alias Craft.RPC.AppendEntries

  require Logger

  import Craft.Tracing, only: [logger_metadata: 1]

  @num_rounds 100

  defstruct [
    :current_round_sent_at,
    latest_successful_round_sent_at: nil,
    rounds: []
  ]

  def new do
    %__MODULE__{}
  end

  def start_new_round(%State{} = state) do
    quorum_status = state.leader_state.quorum_status

    # the monotonic clock is not strictly increasing, so it can freeze unboundedly.
    # if that happens, we add a millisecond to the last round's value to continue generating unique consensus round ids
    now = :erlang.monotonic_time(:millisecond)
    round_sent_at =
      if !quorum_status.current_round_sent_at || quorum_status.current_round_sent_at < now do
        now
      else
        quorum_status.current_round_sent_at + 1
      end

    # drop old rounds
    rounds = Enum.take(quorum_status.rounds, @num_rounds - 1)

    quorum_status =
      %{quorum_status |
        current_round_sent_at: round_sent_at,
        rounds: [{round_sent_at, %{}} | rounds]
      }

    put_in(state.leader_state.quorum_status, quorum_status)
  end

  # {should_handle?, round_is_most_recent_and_just_succeeded?, follower_lagging?, state}
  def heartbeat_response_received(%State{} = state, %AppendEntries.Results{} = results) do
    received_at = :erlang.monotonic_time(:millisecond)
    quorum_status = state.leader_state.quorum_status
    # -1 since we're the leader
    num_replies_needed = State.quorum_needed(state) - 1

    result =
      Enum.reduce_while(state.leader_state.quorum_status.rounds, {[], state.leader_state.quorum_status.rounds}, fn {round_sent_at, heartbeats}, {recent_rounds, rest} ->
        rest = if rest == [], do: rest, else: tl(rest)

        if heartbeats[results.from] do
          if results.heartbeat_sent_at == round_sent_at do
            Logger.warning("duplicate heartbeat reply received: #{inspect results}, ignoring.", logger_metadata(state))
          else
            Logger.warning("out-of-order heartbeat reply received: #{inspect results}, ignoring.", logger_metadata(state))
          end

          {:halt, {false, false, false, quorum_status}}
        else
          if round_sent_at == results.heartbeat_sent_at do
            heartbeats = Map.put(heartbeats, results.from, {results.heartbeat_sent_at, received_at})
            round_successful? = Enum.count(heartbeats) >= num_replies_needed
            round_is_most_recent_and_just_succeeded? = round_successful? and (!quorum_status.latest_successful_round_sent_at ||  round_sent_at > quorum_status.latest_successful_round_sent_at)
            follower_lagging? = round_sent_at != quorum_status.current_round_sent_at

            if follower_lagging? do
              Logger.warning("heartbeat from lagging follower: #{inspect results}.", logger_metadata(state))
            end

            quorum_status =
              if round_is_most_recent_and_just_succeeded? do
                %{quorum_status | latest_successful_round_sent_at: results.heartbeat_sent_at}
              else
                quorum_status
              end

            rounds = List.flatten([Enum.reverse(recent_rounds), {round_sent_at, heartbeats}, rest])

            {:halt, {true, round_is_most_recent_and_just_succeeded?, follower_lagging?, %{quorum_status | rounds: rounds}}}
          else
            {:cont, {[{round_sent_at, heartbeats} | recent_rounds], rest}}
          end
        end
      end)

    case result do
      {_, []} ->
        Logger.warning("expired quorum round in heartbeat reply: #{inspect results}, ignoring.", logger_metadata(state))

        {false, false, false, state}

      {should_handle?, should_notify?, follower_lagging?, quorum_status} ->
        {should_handle?, should_notify?, follower_lagging?, put_in(state.leader_state.quorum_status, quorum_status)}
    end
  end
end
