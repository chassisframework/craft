defmodule Craft.Consensus.State.LeaderState.QuorumStatus do
  alias Craft.Consensus
  alias Craft.Consensus.State
  alias Craft.Message.AppendEntries

  require Logger

  import Craft.Tracing, only: [logger_metadata: 1, telemetry: 3]

  @num_rounds 100
  @rounds_until_idle 10

  defstruct [
    :current_round_sent_at,
    latest_successful_round_sent_at: nil,
    rounds: []
  ]

  defmodule Round do
    defstruct [
      :round_sent_at,
      :empty_write_buffer?,
      heartbeats: %{}
    ]
  end

  def new do
    %__MODULE__{}
  end

  def idle?(%State{} = state) do
    state.leader_state.quorum_status.rounds
    |> Enum.take(@rounds_until_idle)
    |> Enum.all?(fn round -> round.empty_write_buffer? end)
  end

  def start_new_round(%State{} = state, empty_write_buffer?) do
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

    new_round = %Round{round_sent_at: round_sent_at, empty_write_buffer?: empty_write_buffer?}

    # drop old rounds
    rounds = Enum.take(quorum_status.rounds, @num_rounds - 1)

    quorum_status =
      %{quorum_status |
        current_round_sent_at: round_sent_at,
        rounds: [new_round | rounds]
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
      Enum.reduce_while(state.leader_state.quorum_status.rounds, {[], state.leader_state.quorum_status.rounds}, fn round, {recent_rounds, rest} ->
        rest = if rest == [], do: rest, else: tl(rest)

        if round.heartbeats[results.from] do
          if results.heartbeat_sent_at == round.round_sent_at do
            Logger.warning("duplicate heartbeat reply received: #{inspect results}, ignoring.", logger_metadata(state))
            telemetry([:craft, :heartbeat, :reply, :duplicate],
                      %{},
                      %{leader: node(), follower: results.from})
          else
            Logger.warning("out-of-order heartbeat reply received: #{inspect results}, ignoring.", logger_metadata(state))
            telemetry([:craft, :heartbeat, :reply, :out_of_order],
                      %{},
                      %{leader: node(), follower: results.from})
          end

          {:halt, {false, false, false, quorum_status}}
        else
          if round.round_sent_at == results.heartbeat_sent_at do
            heartbeats = Map.put(round.heartbeats, results.from, {results.heartbeat_sent_at, received_at})
            round_successful? = Enum.count(heartbeats) >= num_replies_needed
            round_is_most_recent_and_just_succeeded? = round_successful? and (!quorum_status.latest_successful_round_sent_at ||  round.round_sent_at > quorum_status.latest_successful_round_sent_at)
            follower_lagging? = round.round_sent_at != quorum_status.current_round_sent_at

            if follower_lagging? do
              lag = received_at - round.round_sent_at - Consensus.heartbeat_interval()

              Logger.warning("heartbeat reply from lagging follower, lag=#{lag}ms: #{inspect results}.", logger_metadata(state))

              telemetry([:craft, :heartbeat, :reply, :missed_deadline],
                        %{lag_ms: lag},
                        %{leader: node(), follower: results.from})
            end

            quorum_status =
              if round_is_most_recent_and_just_succeeded? do
                duration = received_at - round.round_sent_at
                breathing_room = Consensus.heartbeat_interval() - duration
                telemetry([:craft, :quorum, :succeeded],
                          %{duration_ms: duration, breathing_room_ms: breathing_room},
                          %{leader: node()})

                %{quorum_status | latest_successful_round_sent_at: results.heartbeat_sent_at}
              else
                quorum_status
              end

            rounds = List.flatten([Enum.reverse(recent_rounds), %{round | heartbeats: heartbeats}, rest])

            {:halt, {true, round_is_most_recent_and_just_succeeded?, follower_lagging?, %{quorum_status | rounds: rounds}}}
          else
            {:cont, {[round | recent_rounds], rest}}
          end
        end
      end)

    case result do
      {_, []} ->
        Logger.warning("expired quorum round in heartbeat reply: #{inspect results}, ignoring.", logger_metadata(state))
        telemetry([:craft, :heartbeat, :reply, :round_expired],
                  %{},
                  %{leader: node(), follower: results.from})

        {false, false, false, state}

      {should_handle?, should_notify?, follower_lagging?, quorum_status} ->
        {should_handle?, should_notify?, follower_lagging?, put_in(state.leader_state.quorum_status, quorum_status)}
    end
  end
end
