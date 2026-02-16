defmodule Craft.Consensus do
  @moduledoc false

  #              |
  #              |
  #              |
  #              v
  # --------> lonely ------> follower -> lonely -> candidate -> leader
  # |                      |
  # |                      |
  # L- receiving_snapshot <-
  #
  # receiving_snapshot: receiving snapshot from the (possibly no longer) leader, never votes
  #                     won't leave this state until snapshot transfer completes
  # follower: happily following leader
  # lonely: hasn't heard from leader in a while (or ever), would be willing to vote 'yes' in a non-transfer election
  # candidate: follower that's called an election after a successful majority pre-vote
  # leader: candidate that's won majority vote

  alias Craft.Consensus.State
  alias Craft.Consensus.State.LeaderState
  alias Craft.Consensus.State.LeaderState.LeadershipTransfer
  alias Craft.Consensus.State.LeaderState.MembershipChange
  alias Craft.Consensus.State.Members
  alias Craft.GlobalTimestamp
  alias Craft.Log.CommandEntry
  alias Craft.Log.MembershipEntry
  alias Craft.Machine
  alias Craft.MemberCache
  alias Craft.Persistence
  alias Craft.Persistence.Metadata
  alias Craft.Message
  alias Craft.Message.AppendEntries
  alias Craft.Message.InstallSnapshot
  alias Craft.Message.RequestVote
  alias Craft.SnapshotServerClient

  require Logger

  import Craft.Tracing, only: [logger_metadata: 1, logger_metadata: 2, time: 3, telemetry: 3]
  import Craft.Application, only: [via: 2]

  @behaviour :gen_statem

  def heartbeat_interval, do: Application.get_env(:craft, :heartbeat_interval, 100)
  def checkquorum_interval, do: Application.get_env(:craft, :checkquorum_interval, 1000)
  def lonely_timeout, do: Application.get_env(:craft, :lonely_timeout, 1000)
  def leader_lease_period, do: Application.get_env(:craft, :leader_lease_period, 800)
  def election_timeout, do: Application.get_env(:craft, :election_timeout, 1500)
  def election_timeout_jitter, do: Application.get_env(:craft, :election_timeout_jitter, 1500)
  def leadership_transfer_timeout, do: Application.get_env(:craft, :leadership_transfer_timeout, 3000)
  def maximum_log_length, do: Application.get_env(:craft, :maximum_log_length, 10_000)
  def maximum_entries_per_heartbeat, do: Application.get_env(:craft, :maximum_entries_per_heartbeat, 1_000)

  if Mix.env == :test do
    # min floor prevents some flake in election tests
    defp lonely_election_timeout(), do: 100 + :rand.uniform(election_timeout_jitter())
  else
    defp lonely_election_timeout(), do: :rand.uniform(election_timeout_jitter())
  end

  #
  # API
  #

  def command(name, command) do
    do_operation(:call, name, {:command, command})
  end

  def state(name, node) do
    remote_operation(name, node, :call, :state)
  end

  def configuration(name, node) do
    remote_operation(name, node, :call, :configuration)
  end

  def transfer_leadership(name, node, to_node) do
    cast_user_command(name, node, {:transfer_leadership, to_node})
  end

  def transfer_leadership(name, node) do
    cast_user_command(name, node, :transfer_leadership)
  end

  def step_down(name, node) do
    remote_operation(name, node, :cast, :step_down)
  end

  def snapshot_ready(name, index, path) do
    do_operation(:call, name, {:snapshot_ready, index, path})
  end

  def backup(name, to_directory) do
    do_operation(:call, name, {:backup, to_directory})
  end

  def get_log(name) do
    do_operation(:call, name, :get_log)
  end

  # casting a user command is necessary when the node that recieves the command
  # is not the one that will respond to it (just leadership transfer, so far).
  def cast_user_command(name, node, msg, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    id = {self(), make_ref()}

    remote_operation(name, node, :cast, {:user_command, id, msg})

    receive do
      {^id, reply} ->
        reply

      after
        timeout ->
          {:error, :timeout}
    end
  end

  # we can't use the {name, node} form, since `name` must be an atom, and we allow anything to be a group name
  # so we use the component registry on the remote node
  def remote_operation(name, node, operation, msg) do
    :rpc.call(node, __MODULE__, :do_operation, [operation, name, msg])
  end
  def do_operation(:cast, name, msg), do: :gen_statem.cast(via(name, __MODULE__), msg)
  def do_operation(:call, name, msg), do: :gen_statem.call(via(name, __MODULE__), msg)

  #
  # genstatem implementation
  #

  def callback_mode, do: [:state_functions, :state_enter]

  def start_link(args) do
    :gen_statem.start_link(via(args.name, __MODULE__), __MODULE__, args, [])
  end

  def init(args) do
    Logger.metadata(name: args.name, node: node(), nexus: args[:nexus_pid])

    if nexus_pid = args[:nexus_pid] do
      remote_group_leader = :rpc.call(node(nexus_pid), Process, :whereis, [:init])
      :logger.update_process_metadata(%{gl: remote_group_leader})
    end

    data = State.new(args.name, args[:nodes], args.persistence, args.machine, args[:global_clock], args[:nexus_pid])

    me = self()
    # avoids passing `state` into the cleaner-upper process
    persistence = data.persistence
    spawn_link(fn ->
      Process.flag(:trap_exit, true)

      receive do
        {:EXIT, ^me, _reason} -> 
          Persistence.close(persistence)

        msg ->
          Logger.warning("consensus cleaner-upper ignored an unknown message: #{inspect msg}")
      end
    end)

    if args[:manual_start] do
      {:ok, :waiting_to_start, data}
    else
      {:ok, :lonely, continue_init(data)}
    end
  end

  defp continue_init(data) do
    MemberCache.non_leader_update(data)

    {:ok, snapshot} = Machine.init_or_restore(data)

    if data.global_clock do
      Logger.info("consensus process started, global clock present, leader leases enabled", logger_metadata(data))
    else
      Logger.info("consensus process started, no global clock present, leader leases disabled", logger_metadata(data))
    end

    %{data | snapshot: snapshot}
  end

  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [args]}
    }
  end

  #
  # manual start (test only)
  #

  if Mix.env() == :test do
    def waiting_to_start(:enter, _, _data), do: :keep_state_and_data
    def waiting_to_start(:cast, :run, data) do
      data = continue_init(data)

      {:next_state, data.state, data, []}
    end
    def waiting_to_start({:call, from}, :state, data) do
      {:keep_state_and_data, [{:reply, from, {data, Persistence.dump(data.persistence)}}]}
    end
  end

  #
  # Lonely
  #
  # hasn't heard from a leader in a while, will:
  #
  # - vote for candidates
  # - hold pre-vote elections as a precursor to becoming a candidate for a true election
  #

  def lonely(:enter, _previous_state, data) do
    data = State.become_lonely(data)

    telemetry([:craft, :role_change, :lonely], %{}, %{})

    Machine.update_role(data)

    Logger.info("became lonely", logger_metadata(data, trace: {:became, :lonely}))

    {:keep_state, data, [{:state_timeout, lonely_election_timeout(), :begin_pre_vote}]}
  end

  def lonely(:state_timeout, :begin_pre_vote, data) do
    Logger.info("pre-vote started", logger_metadata(data, trace: :pre_vote_started))

    Message.request_vote(data, pre_vote: true)

    {:keep_state_and_data, [{:state_timeout, election_timeout(), :election_failed}]}
  end

  def lonely(:state_timeout, :election_failed, data) do
    Logger.info("pre-vote failed, repeating state", logger_metadata(data, trace: :pre_vote_failed))

    :repeat_state_and_data
  end

  # if we receive a message with a higher term, bump our term and process the message
  #
  # this would have been implemented as a `:postpone`, but :gen_statem doesn't process postpones for :repeat_state so we have to fake it
  def lonely(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term > current_term do
    lonely(:cast, msg, State.become_lonely(data, term))
  end

  def lonely(:cast, %RequestVote{pre_vote: true} = request_vote, data) do
    vote_granted = State.vote_for?(data, request_vote)

    Logger.info("#{if vote_granted, do: "granting", else: "denying"} pre-vote to #{request_vote.candidate_id}", logger_metadata(data, trace: {:vote_requested, request_vote, granted?: vote_granted}))

    Message.respond_vote(request_vote, vote_granted, data)

    {:keep_state, data}
  end

  def lonely(:cast, %RequestVote{pre_vote: false} = request_vote, %State{voted_for: nil} = data) do
    {vote_granted, data} =
      if State.vote_for?(data, request_vote) do
        {true, State.set_current_term(data, data.current_term, request_vote.candidate_id)}
      else
        {false, data}
      end

    Logger.debug("#{if vote_granted, do: "granting", else: "denying"} vote to #{request_vote.candidate_id}", logger_metadata(data, trace: {:vote_requested, request_vote, granted?: vote_granted}))

    Message.respond_vote(request_vote, vote_granted, data)

    {:keep_state, data}
  end

  def lonely(:cast, %RequestVote{pre_vote: false} = request_vote, data) do
    Logger.info("denying vote to #{request_vote.candidate_id}, already voted in this term", logger_metadata(data, trace: {:vote_requested, request_vote, granted?: false}))

    Message.respond_vote(request_vote, false, data)

    {:keep_state, data}
  end

  def lonely(:cast, %RequestVote.Results{pre_vote: true} = results, data) do
    Logger.info("pre-vote #{if results.vote_granted, do: "granted", else: "denied"} by #{results.from}", logger_metadata(data, trace: {:vote_received, granted?: results.vote_granted, pre_vote?: true}))

    data = State.record_vote(data, results)

    case State.election_result(data) do
      :won ->
        Logger.info("won pre-vote election", logger_metadata(data, trace: :won_pre_vote_election))

        {:next_state, :candidate, data}

      :lost ->
        Logger.info("lost pre-vote election", logger_metadata(data, trace: :lost_pre_vote_election))

        {:repeat_state, data}

      :pending ->
        {:keep_state, data}
    end
  end

  def lonely(:cast, %AppendEntries{}, data) do
    {:next_state, :follower, data, [:postpone]}
  end

  def lonely(:cast, %InstallSnapshot{}, data) do
    {:next_state, :receiving_snapshot, data, [:postpone]}
  end

  def lonely(:cast, {:user_command, {caller_pid, _ref} = id, _command}, data) do
    send(caller_pid, {id, not_leader_response(data)})

    :keep_state_and_data
  end

  def lonely({:call, from}, :state, data) do
    {:keep_state_and_data, [{:reply, from, {data, Persistence.dump(data.persistence)}}]}
  end

  def lonely({:call, from}, {:snapshot_ready, index, path}, data) do
    data = State.snapshot_ready(data, index, path)

    {:keep_state, data, [{:reply, from, :ok}]}
  end

  def lonely({:call, from}, {:backup, to_directory}, data) do
    backup(to_directory, from, data)
  end

  def lonely({:call, from}, :get_log, data) do
    get_log(from, data)
  end

  def lonely({:call, from}, _request, data) do
    {:keep_state_and_data, [{:reply, from, not_leader_response(data)}]}
  end

  def lonely(type, msg, data) do
    Logger.debug("ignoring #{inspect type} message #{inspect msg}", logger_metadata(data, trace: {:ignored_msg, msg}))

    :keep_state_and_data
  end

  #
  # Receiving Snapshot
  #
  # being sent a snapshot from the (possibly unseated) leader.
  #
  # TODO: consider case where a follower is receiving a snapshot and an election occurs, the new leader may have truncated its log to the point where
  #       the snapshot that's currently being transferred from an the leader may be abandoned and re-started from the current leader. the new leader
  #       wouldn't have the log gap between the snapshot point and the leader's oldest non-compacted log entry.
  #

  def receiving_snapshot(:enter, _previous_state, data) do
    data = State.become_receiving_snapshot(data)

    telemetry([:craft, :role_change, :receiving_snapshot], %{}, %{})

    Machine.update_role(data)

    Logger.info("became receiving_snapshot follower", logger_metadata(data, event: {:became, :receiving_snapshot}))

    {:keep_state, data, []}
  end

  def receiving_snapshot(:cast, %InstallSnapshot{snapshot_transfer: s} = install_snapshot, %State{incoming_snapshot_transfer: {_pid, s}} = data) do
    Logger.debug("ignoring #{inspect install_snapshot.__struct__} message", logger_metadata(data, trace: {:ignored_msg, install_snapshot}))

    data =
      %{data | leader_id: install_snapshot.leader_id}
      |> State.set_current_term(install_snapshot.term)

    MemberCache.non_leader_update(data)

    {:keep_state, data}
  end

  def receiving_snapshot(:cast, %InstallSnapshot{} = install_snapshot, %State{} = data) do
    Logger.debug("receiving snapshot", logger_metadata(data, trace: {:receiving_snapshot, install_snapshot}))

    data =
      %{data | leader_id: install_snapshot.leader_id}
      |> State.set_current_term(install_snapshot.term)

    MemberCache.non_leader_update(data)

    if data.machine.__craft_mutable__() do
      case data.incoming_snapshot_transfer do
        {_old_pid, _old_transfer} ->
          # FIXME: abort current transfer
          :abort

        nil ->
          :noop
      end

      {:ok, data_dir} = Machine.prepare_to_receive_snapshot(data.name)

      me = self()
      {:ok, pid} =
        SnapshotServerClient.start_link(
          install_snapshot.snapshot_transfer,
          data_dir,
          fn
            :ok ->
              :gen_statem.cast(me, {:download_succeeded, install_snapshot})

            error ->
              :gen_statem.cast(me, {:download_failed, error})
          end
        )

      {:keep_state, %{data | incoming_snapshot_transfer: {pid, install_snapshot.snapshot_transfer}}}
    else
      receiving_snapshot(:cast, {:download_succeeded, install_snapshot}, data)
    end
  end

  def receiving_snapshot(:cast, {:download_succeeded, %InstallSnapshot{} = install_snapshot}, %State{} = data) do
    Logger.debug("snapshot download succeeded", logger_metadata(data, trace: :snapshot_download_succeeded))

    persistence = Persistence.truncate(data.persistence, install_snapshot.log_index, install_snapshot.log_entry)

    :ok = Machine.receive_snapshot(data.name, install_snapshot)

    Message.respond_install_snapshot(install_snapshot, true, data)

    {:next_state, :follower, %{data | persistence: persistence}}
  end

  def receiving_snapshot(:cast, {:download_failed, reason}, %State{} = data) do
    Logger.error("error receiving snapshot because: #{inspect reason}, retrying.", logger_metadata(data, trace: {:error_receiving_snapshot, reason}))

    #TODO: delay?

    {:repeat_state, %{data | incoming_snapshot_transfer: nil}}
  end

  def receiving_snapshot(:cast, {:user_command, {caller_pid, _ref} = id, _command}, data) do
    send(caller_pid, {id, not_leader_response(data)})

    :keep_state_and_data
  end

  def receiving_snapshot({:call, from}, {:backup, _to_directory}, _data) do
    {:keep_state_and_data, [{:reply, from, {:error, :receiving_snapshot}}]}
  end

  def receiving_snapshot({:call, from}, :get_log, data) do
    get_log(from, data)
  end

  def receiving_snapshot({:call, from}, _request, data) do
    {:keep_state_and_data, [{:reply, from, not_leader_response(data)}]}
  end

  #
  # Follower
  #
  # hears heartbeats from the leader, appends log entries
  #

  def follower(:enter, _previous_state, data) do
    data = State.become_follower(data)

    telemetry([:craft, :role_change, :follower], %{}, %{})

    Machine.update_role(data)

    Logger.info("became follower", logger_metadata(data, trace: {:became, :follower}))

    {:keep_state, data, [become_lonely_timeout()]}
  end

  def follower(:state_timeout, :become_lonely, data), do: {:next_state, :lonely, data}

  # we just installed this snapshot, the leader hasn't heard yet, ignore it.
  def follower(:cast, %InstallSnapshot{snapshot_transfer: s}, %State{incoming_snapshot_transfer: {_pid, s}}) do
    :keep_state_and_data
  end

  def follower(:cast, %RequestVote{leadership_transfer: true, term: term} = request_vote, %State{current_term: current_term} = data) when term > current_term do
    {vote_granted, data} =
      if State.vote_for?(data, request_vote) do
        {true, State.set_current_term(data, term, request_vote.candidate_id)}
      else
        {false, data}
      end

    Logger.info("#{if vote_granted, do: "granting", else: "denying"} leadership transfer vote to #{request_vote.candidate_id}", logger_metadata(data, trace: {:vote_requested, request_vote, granted?: vote_granted}))

    Message.respond_vote(request_vote, vote_granted, data)

    {:keep_state, data}
  end

  # followers are happy with the leader, they vote "no" to all non-transfer elections types, regardless of term
  def follower(:cast, %RequestVote{} = request_vote, data) do
    Logger.info("denying #{if request_vote.pre_vote, do: "pre-", else: ""}vote to #{request_vote.candidate_id}", logger_metadata(data, trace: {:vote_requested, request_vote, granted?: false, reason: "not lonely"}))

    Message.respond_vote(request_vote, false, data)

    :keep_state_and_data
  end

  # if we receive an AppendEntries message with a higher term, bump our term and process the message
  #
  # this would have been implemented as a `:postpone`, but :gen_statem doesn't process postpones for :repeat_state so we have to fake it
  def follower(:cast, %AppendEntries{term: term} = msg, %State{current_term: current_term} = data) when term > current_term do
    follower(:cast, msg, State.set_current_term(data, term))
  end

  # TODO: move most of this into State module?
  def follower(:cast, %AppendEntries{} = append_entries, data) do
    prev_log_term = append_entries.prev_log_term
    old_commit_index = data.commit_index
    data = %{data | leader_id: append_entries.leader_id}

    data =
      if append_entries.lease_expires_at != data.lease_expires_at do
        %{data | lease_expires_at: append_entries.lease_expires_at}
        |> Metadata.buffer_put()
      else
        data
      end

    {success, data} =
      if Enum.empty?(append_entries.entries) do
        {true, data}
      else
        data = put_in(data.notified_machine_of_idleness, false)

        case Persistence.fetch(data.persistence, append_entries.prev_log_index) do
          {:ok, %{term: ^prev_log_term}} ->
            rewound_entries = Persistence.fetch_from(data.persistence, append_entries.prev_log_index + 1)

            persistence = Persistence.buffer_rewind(data.persistence, append_entries.prev_log_index)

            persistence =
              Enum.reduce(append_entries.entries, persistence, fn entry, persistence ->
                {persistence, _index} =  Persistence.buffer_append(persistence, entry)

                persistence
              end)

            data = %{data | persistence: Persistence.commit_buffer(persistence)}

            new_membership_entry =
              append_entries.entries
              |> Enum.reverse()
              |> Enum.find(fn
                %MembershipEntry{} ->
                  true

                _ ->
                  false
              end)

            case new_membership_entry do
              %MembershipEntry{members: members} ->
                {true, %{data | members: members}}

              # if the entries that we've rewound contained a membership entry, and the incoming entries from the
              # leader don't include a new membership entry, we need to look back through the log until we find one
              # to determine the current cluster membership (section 4.1)
              nil ->
                Enum.find_value(rewound_entries, {true, data}, fn
                  %MembershipEntry{members: members} ->
                    {true, %{data | members: members}}

                  _ ->
                    false
                end)
            end

          _ ->
            {false, %{data | persistence: Persistence.rewind(data.persistence, append_entries.prev_log_index - 1)}}
        end
      end

    data = %{data | commit_index: min(append_entries.leader_commit, Persistence.latest_index(data.persistence))}

    Logger.debug("leader heartbeat from #{append_entries.leader_id}, restarting timer", logger_metadata(data))

    Message.respond_append_entries(append_entries, success, data)

    if success && data.commit_index > old_commit_index do
      log_too_long = Persistence.length(data.persistence) > maximum_log_length()

      Logger.debug("quorum reached", logger_metadata(data, trace: :quorum_reached))

      Machine.quorum_reached(data, log_too_long)
    end

    # we do this after notifying the machine that quorum has been reached so it can bump the commit index
    data =
      if Enum.empty?(append_entries.entries) do
        if !data.notified_machine_of_idleness do
          Machine.notify_idle(data)

          put_in(data.notified_machine_of_idleness, true)
        else
          data
        end
      else
        put_in(data.notified_machine_of_idleness, false)
      end

    MemberCache.non_leader_update(data)

    # leader told us to take over leadership if our log is caught up
    if append_entries.leadership_transfer &&
       append_entries.leadership_transfer.latest_index == Persistence.latest_index(data.persistence) &&
       append_entries.leadership_transfer.latest_term == Persistence.latest_term(data.persistence) do
      {:next_state, :candidate, %{data | leadership_transfer_request_id: append_entries.leadership_transfer.from}}
    else
      {:keep_state, data, [become_lonely_timeout()]}
    end
  end

  def follower(:cast, %InstallSnapshot{}, data) do
    {:next_state, :receiving_snapshot, data, [:postpone]}
  end

  def follower(:cast, {:user_command, {caller_pid, _ref} = id, _command}, data) do
    send(caller_pid, {id, not_leader_response(data)})

    :keep_state_and_data
  end

  def follower({:call, from}, :state, data) do
    {:keep_state_and_data, [{:reply, from, {data, Persistence.dump(data.persistence)}}]}
  end

  def follower({:call, from}, {:snapshot_ready, index, path_or_content}, data) do
    data = State.snapshot_ready(data, index, path_or_content)

    {:keep_state, data, [{:reply, from, :ok}]}
  end

  def follower({:call, from}, {:backup, to_directory}, data) do
    backup(to_directory, from, data)
  end

  def follower({:call, from}, :get_log, data) do
    get_log(from, data)
  end

  def follower({:call, from}, _request, data) do
    {:keep_state_and_data, [{:reply, from, not_leader_response(data)}]}
  end

  def follower(type, msg, data) do
    Logger.debug("ignoring #{inspect type} message #{inspect msg}", logger_metadata(data, trace: {:ignored_msg, msg}))

    :keep_state_and_data
  end

  #
  # Candidate
  #

  def candidate(:enter, :follower, %State{leadership_transfer_request_id: id} = data) when is_tuple(id) or id == :internal do
    data = State.become_candidate(data)

    telemetry([:craft, :role_change, :candidate], %{}, %{})

    Machine.update_role(data)

    Logger.info("became candidate, initiating leadership transfer election", logger_metadata(data, trace: {:became, :candidate}))

    Message.request_vote(data, leadership_transfer: true)

    # TODO: if election fails, send :error to leadership_transfer_request_id
    {:keep_state, data, [{:state_timeout, election_timeout(), :election_failed}]}
  end

  def candidate(:enter, _previous_state, data) do
    data = State.become_candidate(data)

    telemetry([:craft, :role_change, :candidate], %{}, %{})

    Machine.update_role(data)

    Logger.info("became candidate", logger_metadata(data, trace: {:became, :candidate}))

    Message.request_vote(data)

    {:keep_state, data, [{:state_timeout, election_timeout(), :election_failed}]}
  end

  def candidate(:state_timeout, :election_failed, data) do
    Logger.info("election failed, becoming lonely", logger_metadata(data, trace: :election_failed))

    {:next_state, :lonely, data}
  end

  # ignore messages from earlier terms
  def candidate(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term < current_term do
    Logger.debug("ignoring #{inspect msg.__struct__} message #{inspect msg}", logger_metadata(data, trace: {:ignored_msg, msg}))

    :keep_state_and_data
  end

  # become follower if any message from a higher term arrives
  def candidate(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term > current_term, do: become_lonely(msg, data)

  # refuse to vote for another candidate
  def candidate(:cast, %RequestVote{} = request_vote, data) do
    Logger.info("denying #{(if request_vote.pre_vote, do: "pre-", else: "")}vote to #{request_vote.candidate_id}", logger_metadata(data, trace: {:vote_received, granted?: false, pre_vote?: request_vote.pre_vote}))

    Message.respond_vote(request_vote, false, data)

    :keep_state_and_data
  end

  # if a competing candidate becomes leader, convert to follower and process the AppendEntries
  def candidate(:cast, %AppendEntries{}, data) do
    {:next_state, :follower, data, [:postpone]}
  end

  def candidate(:cast, %RequestVote.Results{} = results, data) do
    if results.vote_granted do
      Logger.info("vote granted by #{results.from}", logger_metadata(data, trace: {:vote_received, granted?: results.vote_granted}))
    else
      Logger.info("vote denied by #{results.from}", logger_metadata(data, trace: {:vote_received, granted?: results.vote_granted}))
    end

    # keep the future-most lease timestamp
    latest_leader_lease =
      case {data.lease_expires_at, results.lease_expires_at} do
        {nil, nil} ->
          nil

        {nil, %GlobalTimestamp{} = follower_ts} ->
          follower_ts

        {%GlobalTimestamp{} = our_ts, nil} ->
          our_ts

        {%GlobalTimestamp{} = our_ts, %GlobalTimestamp{} = follower_ts} ->
          if DateTime.compare(our_ts.latest, follower_ts.latest) == :lt do
            follower_ts
          else
            our_ts
          end
      end

    data =
      %{data | lease_expires_at: latest_leader_lease}
      |> Metadata.write()
      |> State.record_vote(results)

    case State.election_result(data) do
      :won ->
        Logger.info("election won, becoming leader", logger_metadata(data, trace: :election_won))
        {:next_state, :leader, data}

      :lost ->
        Logger.info("election lost, becoming lonely", logger_metadata(data, trace: :election_lost))
        {:next_state, :lonely, data}

      :pending ->
        {:keep_state, data}
    end
  end

  # even though this node doesn't recognize the leader as legitimate anymore,
  # we should still try to redirect commands to the old leader, in case it
  # actually is legitimate and we're incorrect. (e.g. we're isolated from the
  # other nodes and they're happily carrying on without us)
  def candidate(:cast, {:user_command, {caller_pid, _ref} = id, _command}, data) do
    send(caller_pid, {id, not_leader_response(data)})

    :keep_state_and_data
  end

  def candidate({:call, from}, :state, data) do
    {:keep_state_and_data, [{:reply, from, {data, Persistence.dump(data.persistence)}}]}
  end

  def candidate({:call, from}, {:snapshot_ready, index, path_or_content}, data) do
    data = State.snapshot_ready(data, index, path_or_content)

    {:keep_state, data, [{:reply, from, :ok}]}
  end

  def candidate({:call, from}, {:backup, to_directory}, data) do
    backup(to_directory, from, data)
  end

  def candidate({:call, from}, :get_log, data) do
    get_log(from, data)
  end

  def candidate({:call, from}, _request, data) do
    {:keep_state_and_data, [{:reply, from, not_leader_response(data)}]}
  end

  def candidate(type, msg, data) do
    Logger.debug("ignoring #{inspect type} message #{inspect msg}", logger_metadata(data, trace: {:ignored_msg, msg}))

    :keep_state_and_data
  end

  #
  # Leader
  #

  def leader(:enter, previous_state, %State{leadership_transfer_request_id: {caller_pid, _ref} = id} = data) do
    send(caller_pid, {id, :ok})

    leader(:enter, previous_state, %{data | leadership_transfer_request_id: nil})
  end

  def leader(:enter, _previous_state, data) do
    data = State.become_leader(data)

    telemetry([:craft, :role_change, :leader], %{}, %{})

    #
    # when the cluster starts up, each node is explicitly given the same configuration to
    # hold in-memory to bootstrap the cluster.
    #
    # however, we need to store the current config in the log so that when nodes restart,
    # they have a way to determine what the current config is (walk backwards from the end of the
    # log looking for the most recent config)
    #
    # appending an entry from the current term also allows us to commit any possibly uncommitted entries
    # from previous terms (section 5.4.2)
    #
    # even if we don't hold the lease, we still proceed with committing this no-op entry, and hence with committing
    # previous terms' entries and forcing followers logs to conform to ours. i believe this is safe, because (by design)
    # the new leader has no idea who asked for what writes to occur, that information is stored emphemerally by the
    # leader's machine and not in the log itself. so any hanging writes (section 5.4.2 operations) that were requested of the
    # previous leader can not be answered by the new leader. since those writes will never be confirmed to the client,
    # they're not "witnessed" state confirmations according to the outside world. and hence they're not a linearizability violation.
    #
    # additionally, we need to wait for this empty entry to commit before we service any reads, since every new leader needs to
    # determine what the correct commit index is for itself. (for example, if we appended an entry from the previous leader,
    # and then leadership was transferred to us, but we hadn't yet heard from the previous leader that the commit index had bumped,
    # we'd still think the commit index was from before that entry, which isn't true)
    #
    data = %{data | persistence: Persistence.append(data.persistence, MembershipEntry.new(data))}
    wait_for_entry_index = Persistence.latest_index(data.persistence)

    Machine.update_role(data, wait_for_entry_index)

    actions = [
      {:state_timeout, 0, :heartbeat},
      {{:timeout, :check_quorum}, checkquorum_interval(), :check_quorum}
    ]

    if data.global_clock do
      if data.lease_expires_at do
        actions = [{{:timeout, :takeover}, 0, data.lease_expires_at} | actions]
        data = put_in(data.leader_state.waiting_for_lease, true)

        Logger.info("became leader, waiting for lease", logger_metadata(data, trace: {:became, :leader}))

        {:keep_state, data, actions}
      else
        Logger.info("became leader, immediately taking lease", logger_metadata(data, trace: {:became, :leader}))

        {:keep_state, data, actions}
      end
    else
      Logger.info("became leader", logger_metadata(data, trace: {:became, :leader}))

      {:keep_state, data, actions}
    end
  end

  def leader(:state_timeout, :heartbeat, data) do
    {:keep_state, heartbeat(data), [{:state_timeout, heartbeat_interval(), :heartbeat}]}
  end

  def leader({:timeout, :check_quorum}, :check_quorum, data) do
    if data.leader_state.quorum_status.latest_successful_round_sent_at < :erlang.monotonic_time(:millisecond) - checkquorum_interval() do
      Logger.info("unable to make quorum, stepping down.", logger_metadata(data, trace: {:check_quorum, :failed}))

      telemetry([:craft, :check_quorum, :failed], %{}, %{})

      {:next_state, :lonely, data}
    else
      Logger.debug("check-quorum successful", logger_metadata(data, trace: {:check_quorum, :ok}))

      telemetry([:craft, :check_quorum, :succeeded], %{}, %{})

      {:keep_state_and_data, [{{:timeout, :check_quorum}, checkquorum_interval(), :check_quorum}]}
    end
  end

  def leader({:timeout, :takeover}, old_lease_expires_at, data) do
    # don't assume that because our lease wait-out event fired that the lease has expired,
    # our monotonic clock could be wrong, the user-provided clock source is the authority
      case GlobalTimestamp.time_until_lease_expires(data.global_clock, old_lease_expires_at) do
        {:ok, 0} ->
          Logger.info("taking over lease", logger_metadata(data, trace: :lease_takeover))

          data = put_in(data.leader_state.waiting_for_lease, false)

          MemberCache.update_lease_holder(data)

          Machine.lease_taken(data)

          {:keep_state, data}

        {:ok, wait_time} ->
          Logger.debug("waiting #{wait_time}ms for lease", logger_metadata(data, trace: {:waiting_out_lease, wait_time}))

          {:keep_state_and_data, [{{:timeout, :takeover}, wait_time, old_lease_expires_at}]}

        error ->
          Logger.warning("unable to acquire lease, global clock error: #{inspect error}, becoming follower", logger_metadata(data, trace: :global_clock_failure))

          {:next_state, :lonely, data}
      end
  end

  # ignore any messages from earlier terms
  def leader(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term < current_term do
    Logger.debug("ignoring #{inspect msg.__struct__} message #{inspect msg}", logger_metadata(data, trace: {:ignored_msg, msg}))

    :keep_state_and_data
  end

  # become follower if any message from a higher term arrives
  def leader(:cast, %{term: term} = msg, %State{current_term: current_term} = data) when term > current_term, do: become_lonely(msg, data)

  def leader(:cast, %RequestVote{pre_vote: true} = request_vote, data) do
    vote_granted = State.vote_for?(data, request_vote)

    Logger.info("#{if vote_granted, do: "granting", else: "denying"} pre-vote to #{request_vote.candidate_id}", logger_metadata(data, trace: {:vote_requested, request_vote, granted?: vote_granted}))

    Message.respond_vote(request_vote, vote_granted, data)

    {:keep_state, data}
  end

  # ignore superfluous votes from when we were a candidate
  def leader(:cast, %RequestVote.Results{}, _data), do: :keep_state_and_data

  def leader(:cast, %AppendEntries.Results{} = results, data) do
    data = LeaderState.handle_append_entries_results(data, results)

    data =
      Enum.reduce(data.members.catching_up_nodes, data, fn node, data ->
        if Persistence.latest_index(data.persistence) <= Map.get(data.leader_state.match_indices, node) do
          Logger.info("node #{inspect node} is caught up", logger_metadata(data))

          data = %{data | members: Members.allow_node_to_vote(data.members, node)}

          %{data | persistence: Persistence.append(data.persistence, MembershipEntry.new(data))}
        else
          data
        end
      end)

    # the membership change has committed
    with %MembershipChange{} = membership_change <- data.leader_state.membership_change,
         true <- data.commit_index >= membership_change.log_index do
      data = put_in(data.leader_state.membership_change, nil)

      # if we're being removed, transfer leadership away first
      if membership_change.action == :remove && membership_change.node == node() do
        data = LeaderState.transfer_leadership(data)

        {:keep_state, heartbeat(data), [{{:timeout, :leadership_transfer_failed}, leadership_transfer_timeout(), :self_removal}]}
      else
        {:keep_state, data}
      end
    else
      _ ->
        {:keep_state, data}
    end
  end

  def leader(:cast, %InstallSnapshot.Results{} = results, data) do
    {:keep_state, LeaderState.handle_install_snapshot_results(data, results)}
  end

  def leader(:cast, :step_down, data) do
    Logger.info("stepping down", logger_metadata(data, trace: :step_down))

    {:next_state, :lonely, data, []}
  end

  def leader(:cast, {:user_command, id, :transfer_leadership}, data) do
    to_node =
      data.members
      |> Members.other_voting_nodes()
      |> Enum.random()

    data = LeaderState.transfer_leadership(data, to_node, id)

    actions = [{{:timeout, :leadership_transfer_failed}, leadership_transfer_timeout(), :user_requested}]

    {:keep_state, heartbeat(data), actions}
  end

  # user asked to transfer leadership to existing leader, noop
  def leader(:cast, {:user_command, {caller_pid, _ref} = id, {:transfer_leadership, to_node}}, _data) when to_node == node() do
    send(caller_pid, {id, :ok})

    :keep_state_and_data
  end

  def leader(:cast, {:user_command, {caller_pid, _ref} = id, {:transfer_leadership, to_node}}, data) do
    if Members.can_vote?(data.members, to_node) do
      data = LeaderState.transfer_leadership(data, to_node, id)

      actions = [{{:timeout, :leadership_transfer_failed}, leadership_transfer_timeout(), :user_requested}]

      {:keep_state, heartbeat(data), actions}
    else
      send(caller_pid, {id, {:error, :not_voting_member}})

      :keep_state_and_data
    end
  end

  def leader({:call, from}, {:command, _command}, %State{leader_state: %LeaderState{leadership_transfer: %LeadershipTransfer{} = leadership_transfer}}) do
    {:keep_state_and_data, [{:reply, from, {:error, {:leadership_transfer_in_progress, leadership_transfer.current_candidate}}}]}
  end

  def leader({:call, from}, {:command, command}, %State{global_clock: global_clock} = data) when not is_nil(global_clock) do
    case GlobalTimestamp.time_until_lease_expires(data.global_clock, data.lease_expires_at) do
      {:ok, time} when time > 0 ->
        handle_command(command, from, data)

      {:ok, 0} ->
        {:keep_state_and_data, [{:reply, from, {:error, :not_leaseholder}}]}

      error ->
        Logger.error("unable to determine global time for command, got #{inspect error}, becoming follower", logger_metadata(data, trace: :global_clock_failure))

        {:next_state, :lonely, data, [{:reply, from, {:error, :not_leaseholder}}]}
    end
  end

  def leader({:call, from}, {:command, command}, data) do
    handle_command(command, from, data)
  end

  def leader({:call, from}, {:snapshot_ready, index, path_or_content}, data) do
    data = State.snapshot_ready(data, index, path_or_content)

    {:keep_state, data, [{:reply, from, :ok}]}
  end

  def leader({:call, from}, {:backup, to_directory}, data) do
    backup(to_directory, from, data)
  end

  def leader({:call, from}, :get_log, data) do
    get_log(from, data)
  end

  def leader({:call, from}, _msg, %State{leader_state: %LeaderState{leadership_transfer: %LeadershipTransfer{} = leadership_transfer}}) do
    {:keep_state_and_data, [{:reply, from, {:error, {:leadership_transfer_in_progress, leadership_transfer.current_candidate}}}]}
  end

  def leader({:call, from}, :configuration, data) do
    config =
      data
      |> Map.take([:members, :nexus_pid, :global_clock])
      |> Map.merge(%{machine_module: data.machine, persistence_module: data.persistence.module})

    {:keep_state_and_data, [{:reply, from, {:ok, config}}]}
  end

  def leader({:call, from}, :state, data) do
    {:keep_state_and_data, [{:reply, from, {data, Persistence.dump(data.persistence)}}]}
  end

  def leader(type, msg, data) do
    Logger.debug("ignoring #{inspect type} message #{inspect msg}", logger_metadata(data, trace: {:ignored_msg, msg}))

    :keep_state_and_data
  end

  defp become_lonely_timeout do
    {:state_timeout, lonely_timeout(), :become_lonely}
  end

  defp become_lonely(%{term: term}, data) do
    {:next_state, :lonely, State.set_current_term(data, term), [:postpone]}
  end

  defp not_leader_response(%State{leader_id: nil}), do: {:error, :unknown_leader}
  defp not_leader_response(%State{leader_id: leader_id}), do: {:error, {:not_leader, leader_id}}

  defp heartbeat(%State{} = state) do
    time(fn ->
      state = LeaderState.QuorumStatus.start_new_round(state, not Persistence.any_buffered_log_writes?(state.persistence))

      state =
        if LeaderState.QuorumStatus.idle?(state) do
          if !state.notified_machine_of_idleness do
            Machine.notify_idle(state)

            put_in(state.notified_machine_of_idleness, true)
          else
            state
          end
        else
          put_in(state.notified_machine_of_idleness, false)
        end

      state =
        if state.global_clock do
          case GlobalTimestamp.now(state.global_clock) do
            {:ok, now} ->
              # if we're within three heartbeats of lease expiration, bump the lease
              # we don't bump the lease with every heartbeat as it costs latency to write the new lease to disk
              if !state.lease_expires_at or DateTime.diff(state.lease_expires_at.earliest, now.latest, :millisecond) / heartbeat_interval() < 3 do
                %{state | lease_expires_at: GlobalTimestamp.add(now, leader_lease_period(), :millisecond)}
                |> Metadata.buffer_put()
              else
                state
              end

            error ->
              Logger.error("unable to determine global time, got #{inspect error}, becoming follower", logger_metadata(state, trace: :global_clock_failure))

              throw({:next_state, :lonely, state})
          end
        else
          state
        end

      state = %{state | persistence: Persistence.commit_buffer(state.persistence)}

      state =
        state.members
        |> Members.other_nodes()
        |> Enum.reduce(state, fn to_node, state ->
          if LeaderState.needs_snapshot?(state, to_node) do
            LeaderState.create_snapshot_transfer(state, to_node)
          else
            state
          end
        end)

      logger_metadata = :logger.get_process_metadata()

      state.members
      |> Members.other_nodes()
      |> Task.async_stream(fn to_node ->
        :logger.set_process_metadata(logger_metadata)

        if state.leader_state.snapshot_transfers[to_node] do
          Message.install_snapshot(state, to_node)
        else
          Message.append_entries(state, to_node)
        end
      end, ordered: false)
      |> Stream.run()

      state
    end,
    [:craft, :quorum, :heartbeat],
    %{name: state.name})
  end

  defp handle_command({membership_change, node}, from, data) when membership_change in [:add_member, :remove_member] do
    if LeaderState.config_change_in_progress?(data) do
      {:keep_state_and_data, [{:reply, from, {:error, :config_change_in_progress}}]}
    else
      entry_index = Persistence.latest_index(data.persistence) + 1

      {reply, data} =
        case membership_change do
          :add_member ->
            if node in Members.all_nodes(data.members) do
              {{:error, :already_joined}, data}
            else
              {{:ok, entry_index}, LeaderState.add_node(data, node, entry_index)}
            end

          :remove_member ->
            if node not in Members.all_nodes(data.members) do
              {{:error, :unknown_member}, data}
            else
              {{:ok, entry_index}, LeaderState.remove_node(data, node, entry_index)}
            end
        end

      case reply do
        {:ok, _} ->
          MemberCache.leader_update(data)

          entry = %MembershipEntry{term: data.current_term, members: data.members}
          persistence = Persistence.append(data.persistence, entry)

          {:keep_state, %{data | persistence: persistence}, [{:reply, from, reply}]}

        {:error, _} ->
          {:keep_state_and_data, [{:reply, from, reply}]}
      end
    end
  end

  defp handle_command({:machine_command, command}, from, data) do
    entry = %CommandEntry{term: data.current_term, command: command}

    {persistence, entry_index} = Persistence.buffer_append(data.persistence, entry)

    {:keep_state, %{data | persistence: persistence}, [{:reply, from, {:ok, entry_index}}]}
  end

  defp backup(to_directory, from, data) do
    consensus_result = Persistence.backup(data.persistence, to_directory)
    machine_result = Machine.backup(data.name, Path.join(to_directory, "machine"))

    result =
      if consensus_result == :ok and machine_result == :ok do
        :ok
      else
        {:error, %{machine_result: machine_result, consensus_result: consensus_result}}
      end

    {:keep_state_and_data, [{:reply, from, result}]}
  end

  defp get_log(from, data) do
    {:keep_state_and_data, [{:reply, from, data.persistence}]}
  end
end
