defmodule Craft.Machine do
  @doc false
  use GenServer

  alias Craft.Configuration
  alias Craft.Consensus
  alias Craft.Consensus.State, as: ConsensusState
  alias Craft.Log.CommandEntry
  alias Craft.Log.MembershipEntry
  alias Craft.Log.SnapshotEntry
  alias Craft.MemberCache
  alias Craft.MemberCache.GroupStatus
  alias Craft.Persistence
  alias Craft.SnapshotServer.RemoteFile

  import Craft.Tracing, only: [logger_metadata: 1, time: 3, time: 4]
  import Craft.Application, only: [via: 2, lookup: 2]

  require Logger

  @type private :: any()
  @type snapshot :: any()
  @type role :: :receiving_snapshot | :lonely | :follower | :candidate | :leader
  @type reply_from :: {:direct, GenServer.from()} | {:quorum, query_time :: integer(), pid(), GenServer.from()}

  @callback init(Craft.group_name()) :: {:ok, private()}
  @callback handle_command(Craft.command(), Craft.log_index(), private()) :: {Craft.reply(), private()} | {Craft.reply(), Craft.side_effects(), private()}
  @callback handle_commands([{Craft.command(), Craft.log_index()}], private()) :: {Craft.reply(), private()} | {Craft.reply(), Craft.side_effects(), private()}
  @callback handle_query(Craft.query(), reply_from(), private()) :: {:reply, Craft.reply()} | :noreply
  @callback handle_role_change(role(), private()) :: private()
  @callback handle_lease_taken(private()) :: private()
  @callback handle_info(term(), private()) :: private()

  @optional_callbacks handle_command: 3, handle_commands: 2, handle_role_change: 2, handle_lease_taken: 1, handle_info: 2

  defmodule MutableMachine do
    @type private() :: Craft.Machine.private()
    @type snapshot() :: Craft.Machine.snapshot()
    @type data_dir() :: Path.t()
    @type index() :: pos_integer()

    @callback last_applied_log_index(private()) :: Craft.log_index() | nil
    @callback snapshot(private()) :: {index(), snapshot()} | nil
    @callback snapshots(private()) :: %{index() => snapshot()}
    @callback prepare_to_receive_snapshot(private()) :: {:ok, data_dir(), private()}
    @callback receive_snapshot(private()) :: {:ok, private()}
    @callback backup(to_directory :: Path.t(), private()) :: :ok | {:error, any()}
    @callback close(private()) :: private()
  end

  defmodule LogStoredMachine do
    @type private() :: Craft.Machine.private()
    @type snapshot() :: Craft.Machine.snapshot()

    @callback snapshot(private()) :: snapshot()
    @callback receive_snapshot(snapshot(), private()) :: private()
  end

  defmodule State do
    defstruct [
      :name,
      :module,
      :private,
      :role,
      :global_clock,
      :last_quorum_at,
      :snapshotter_pid, # pathalogical mutex to serialize snapshot-taking
      :idle,
      # we need to wait for the section 5.4.2 entry to commit before servicing client requests
      # boolean | {:waiting, log_index}
      waiting_for_first_commit: true,
      mode: :normal, # :normal | :write_optimized
      last_applied: 0,
      #
      # read-index queries are be a bit tricky with regards to safety, in order to guarantee linearizability, we can only execute the query when our
      # index is greater than or equal to the read-index that we got from the leader when the query began, but less than or equal to the last-applied
      # index of the leader, even if they're committed. if we do, we enter into a three-way race condition betweeen the consensus apparatus and the
      # machines on the leader and the follower: if we (the follower), receive a commit index faster than the machine on the leader (because it's bogged
      # down), we'll apply commands before the leader's machine does, which allows a follower read-index query to give a more recent answer than even a
      # lease read on the leader. so it's critical that we never respond to read-index queries that exceed the last-applied given to us by the leader,
      # even if the commit index is greater. for that reason, the leader communicates its last-applied index to followers via hearbeats, and followers
      # only apply entries up to that index.
      #
      # limiting the application of entries by followers to the last-applied index of the leader is a guaranteed way to preserve linearizability for
      # read-index queries. communication of the commit index is not sufficient when the consensus and machine processes are decoupled.

      # the maximum index up to which this machine is allowed to apply commands (inclusive)
      # - on the leader, it's the commit index
      # - on followers, it's the leader's last_applied
      # this preserves linearizability in read-index queries by preventing followers from applying commits before the leader
      apply_up_to: 0,
      read_index_tasks: %{},
      read_index_queries: :gb_trees.empty(),
      client_query_results: :gb_trees.empty(),
      pending_parallel_queries: MapSet.new(),
      client_commands: %{}
    ]
  end

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: via(args.name, __MODULE__))
  end

  def init_or_restore(%ConsensusState{} = state) do
    state.name
    |> lookup(__MODULE__)
    |> GenServer.call({:init_or_restore, state.persistence})
  end

  def update_role(%ConsensusState{} = state, await_commit_index \\ nil) do
    state.name
    |> lookup(__MODULE__)
    |> GenServer.cast({:update_role, state.state, await_commit_index})
  end

  def prepare_to_receive_snapshot(name) do
    name
    |> lookup(__MODULE__)
    |> GenServer.call(:prepare_to_receive_snapshot)
  end

  def receive_snapshot(name, install_snapshot \\ nil) do
    name
    |> lookup(__MODULE__)
    |> GenServer.call({:receive_snapshot, install_snapshot})
  end

  def backup(name, to_directory) do
    name
    |> lookup(__MODULE__)
    |> GenServer.call({:backup, to_directory})
  end

  def lease_taken(%ConsensusState{} = state) do
    state.name
    |> lookup(__MODULE__)
    |> GenServer.cast(:lease_taken)
  end

  def notify_idle(%ConsensusState{} = state) do
    state.name
    |> lookup(__MODULE__)
    |> GenServer.cast(:idle)
  end

  def send_user_message(name, message) do
    if pid = lookup(name, __MODULE__) do
      GenServer.cast(pid, {:user_message, message})
    else
      {:error, :unknown_group}
    end
  end

  def state(name) do
    name
    |> lookup(__MODULE__)
    |> GenServer.call(:state)
  end

  def state(name, node) do
    :rpc.call(node, __MODULE__, :state, [name])
  end

  # FIXME: document that for persistent machines, the commit index and the
  # state mutations need to be atomically commited to the persistent store
  #
  # otherwise a crash between the two operations would result in inconsistent
  # state when the machine picks up the log where it left off
  #
  # - document that the consensus process sends the `log` state to this process,
  # and that craft assumes that the `log` is small (a handle), rather
  # than the whole log itself, the point of this is to allow the consensus process
  # to continue without being blocked by the machine process while it's applying
  # entries
  #
  def quorum_reached(%ConsensusState{} = state, apply_up_to, should_snapshot?) do
    # nil if not leader
    last_quorum_at = get_in(state.leader_state.quorum_status.latest_successful_round_sent_at)

    state.name
    |> lookup(__MODULE__)
    |> GenServer.cast({:quorum_reached, apply_up_to, state.persistence, should_snapshot?, last_quorum_at})
  end

  def call(name, node, request, timeout) do
    :rpc.call(node, __MODULE__, :do_call, [name, request, timeout])
  end

  def do_call(name, request, timeout) do
    name
    |> lookup(__MODULE__)
    |> GenServer.call(request, timeout)
  end

  def now do
    case Process.get(:__craft_meta__) do
      %{global_clock: global_clock} when not is_nil(global_clock) ->
        global_clock.now()

      _ ->
        raise "no global clock configured"
    end
  end

  def holding_lease? do
    %{group_name: group_name} = Process.get(:__craft_meta__)

    Craft.holding_lease?(group_name)
  end

  @impl true
  def init(args) do
    Logger.metadata(name: args.name, node: node(), nexus: args[:nexus_pid])

    if nexus_pid = args[:nexus_pid] do
      remote_group_leader = :rpc.call(node(nexus_pid), Process, :whereis, [:init])
      :logger.update_process_metadata(%{gl: remote_group_leader})
    end

    Process.put(:__craft_meta__, %{group_name: args.name, global_clock: args[:global_clock]})

    {:ok, %State{name: args.name, module: args.machine, global_clock: args[:global_clock]}}
  end

  @impl true
  def handle_cast({:update_role, new_role, await_commit_index}, state) do
    # if we were just the leader and are holding in-flight requests, but we've been deposed, we need to let the awaiting client know
    {:ok, group_status} = MemberCache.get(state.name)

    # if we're the just-deposed leader, we probably don't know who the new leader is
    response =
      if !group_status.leader || state.role == :leader && node() == group_status.leader && new_role != :leader  do
        {:error, :unknown_leader}
      else
        {:error, {:not_leader, group_status.leader}}
      end

    for {_query_executed_at, from, _result} <- :gb_trees.to_list(state.client_query_results) do
      GenServer.reply(from, response)
    end

    for from <- state.pending_parallel_queries do
      GenServer.reply(from, response)
    end

    for {_index, {from, async_caller}} <- state.client_commands do
      if async_caller do
        {_pid, ref} = from

        send(async_caller, {:"$craft_command", ref, response})
      else
        GenServer.reply(from, response)
      end
    end

    # for a new leader, handle_role_change/2 is called from the :quorum_reached handler when the first section 5.4.2 commit is observed
    private =
      if new_role != :leader and function_exported?(state.module, :handle_role_change, 2) do
        state.module.handle_role_change(new_role, state.private)
      else
        state.private
      end

    waiting_for_first_commit =
      if new_role == :leader do
        Logger.debug("waiting for index #{await_commit_index}", logger_metadata(trace: :waiting))

        {:waiting, await_commit_index}
      else
        false
      end

    {:noreply,
     %{
       state
       | role: new_role,
         private: private,
         client_commands: %{},
         client_query_results: :gb_trees.empty(),
         pending_parallel_queries: MapSet.new(),
         waiting_for_first_commit: waiting_for_first_commit
     }}
  end

  @impl true
  def handle_cast({:quorum_reached, apply_up_to, log, should_snapshot?, last_quorum_at}, state) do
    Logger.debug(fn ->
      metadata = %{}
      metadata = if apply_up_to > state.last_applied, do: Map.put(metadata, :new_apply_up_to, apply_up_to), else: metadata

      {"quorum reached", logger_metadata(trace: {:quorum_reached, metadata})}
    end)

    range_since_last_update = state.apply_up_to+1..apply_up_to//1
    entries_since_last_update = Persistence.fetch_between(log, range_since_last_update)


    {read_index_queries, queries_to_execute} = take_gb_tree(state.read_index_queries, apply_up_to)
    read_index_queries_to_execute = Enum.flat_map(queries_to_execute, &MapSet.to_list/1)
    state = %{
      state |
        read_index_queries: read_index_queries,
        last_quorum_at: last_quorum_at,
        apply_up_to: apply_up_to,
        # if the apply_up_to index wasn't bumped, we're probably idle
        idle: apply_up_to == state.apply_up_to
    }

    state =
      if state.mode == :write_optimized and Enum.all?(entries_since_last_update, &match?(%CommandEntry{}, &1)) do
        Enum.reduce(range_since_last_update, state, fn index, state ->
          reply_to_command(state, index, :ok)
        end)
      else
        apply_outstanding_entries(state, log)
      end

    state =
      if read_index_queries_to_execute != [] do
        # in write-optimized mode, we've already responded to the current batch, but we need to apply everything for follower reads
        state = apply_outstanding_entries(state, log)

        # answer follower read-index queries
        for {from, query} <- read_index_queries_to_execute do
          time(fn ->
            case state.module.handle_query(query, {:direct, from}, state.private) do
              {:reply, reply} ->
                Logger.debug("executing read-index query", logger_metadata(trace: {:sending_read_index_response, %{from: from, query: query, result: reply}}))
                GenServer.reply(from, reply)

              :noreply ->
                :noop
            end
          end,
          [:craft, :machine, :user, :handle_query],
          %{linearizable: true, follower_read: true})
        end

        state
      else
        state
      end

    # answer client queries
    state =
      if state.role == :leader do
        {client_query_results, results_ready} = take_gb_tree(state.client_query_results, last_quorum_at)
        state = %{state | client_query_results: client_query_results}

        for {from, result} <- results_ready do
          Logger.debug("responding to client query", logger_metadata(trace: {:sending_client_response, %{from: from, result: result}}))

          GenServer.reply(from, result)
        end

        state
      else
        state
      end

    # update leader-readiness if user's state machine has caught up post-election.
    state =
      if state.role == :leader do
        case state.waiting_for_first_commit do
          {:waiting, index} when state.last_applied >= index ->
            Logger.debug("saw first commit, ready for queries", logger_metadata(trace: :ready_for_queries))

            private =
              if function_exported?(state.module, :handle_role_change, 2) do
                state.module.handle_role_change(:leader, state.private)
              else
                state.private
              end

            MemberCache.set_leader_ready(state.name, true)

            %{state | waiting_for_first_commit: false, private: private}

          _ ->
            state
        end
      else
        state
      end

    #
    # right now, snapshotting is a synchronous process, later we should allow for async snapshots at
    # the provided index, and the user will call back when it's done (and we'll send a message
    # to the consensus module to tell it that a snapshot at the given index completed)
    #
    # should probably provide sync/async semantics as well
    #
    # only snapshot up to one entry before the latest, since we need the prev log entry to create AppendEntries
    #
    state =
      if should_snapshot? do
        if state.module.__craft_mutable__() do
          if !state.snapshotter_pid or !Process.alive?(state.snapshotter_pid) do
            # apply any outstanding commands before snapshotting (write-optimized mode)
            state = apply_outstanding_entries(state, log)

            snapshotter_pid = spawn_link(fn ->
              time(fn ->
                case state.module.snapshot(state.private) do
                  {index, path} ->
                    Logger.debug("snapshot ready", logger_metadata(trace: :snapshot_ready))

                    :ok = Consensus.snapshot_ready(state.name, index, snapshot_info(path))

                  # machine decided not to snapshot (perhaps no commands have run)
                  nil ->
                    :noop
                end
              end,
              [:craft, :machine, :user, :snapshot],
              %{})
            end)

            %{state | snapshotter_pid: snapshotter_pid}
          else
            state
          end
        else
          Logger.debug("snapshot ready", logger_metadata(trace: :snapshot_ready))

          :ok = Consensus.snapshot_ready(state.name, state.last_applied, state.module.snapshot(state.private))

          state
        end
      else
        state
      end

    {:noreply, state}
  end

  def handle_cast(:lease_taken, state) do
    private =
      if function_exported?(state.module, :handle_lease_taken, 1) do
        state.module.handle_lease_taken(state.private)
      else
        state.private
      end

    {:noreply, %{state | private: private}}
  end

  # do a chunk of idle work, then re-check to see if still idle
  #
  # leader is idle (past N quorum rounds had no log writes)
  # follower is idle when an empty heartbeat is received
  @impl true
  def handle_cast(:idle, %State{mode: :write_optimized} = state) do
    log = Consensus.get_log(state.name)
    end_range = min(state.apply_up_to, state.last_applied + Consensus.maximum_entries_per_heartbeat())

    range = state.last_applied+1..end_range//1
    unapplied_entries = Persistence.fetch_between(log, range)
    unapplied_entries_with_index = Enum.zip(range, unapplied_entries)
    state = apply_entries(state, unapplied_entries_with_index)

    Logger.debug("idle, applying chunk of #{Enum.count(unapplied_entries)} outstanding commands", logger_metadata(trace: {:idle, :applying_commands, Enum.count(unapplied_entries)}))

    if end_range < state.apply_up_to do
      GenServer.cast(self(), :maybe_do_idle_work)
    end

    {:noreply, %{state | idle: true}}
  end
  def handle_cast(:idle, state), do: {:noreply, %{state | idle: true}}

  def handle_cast(:maybe_do_idle_work, %State{idle: true} = state), do: handle_cast(:idle, state)
  def handle_cast(:maybe_do_idle_work, state), do: {:noreply, state}

  def handle_cast({:user_message, msg}, state) do
    private =
      if function_exported?(state.module, :handle_info, 2) do
        state.module.handle_info(msg, state.private)
      else
        Logger.error("Message #{inspect(msg)} received but no handle_info defined in #{inspect(state.module)}")

        state.private
      end

    {:noreply, %{state | private: private}}
  end
  #
  # the leader handles linearizable queries slightly differently, depending on if leases are enabled:
  #   - if leases are enabled,
  #       - and we're within the lease period, the leader knows that it is the current leader, so it immediately processes a query
  #       - and we're outside the lease period, it's likely we've been deposed or failed quorum, return an error.
  #   - if leases are disabled, we need to see a quorum round succeed to know that we were still the leader when the query was executed. so,
  #       we execute the query speculatively and store the result along with the current monotonic time, all the while, the consensus process
  #       is streaming monotonic timestamps of when the most recent quorum was achieved.
  #     - when the quorum timestamp exceeds the query's timestamp, we send the result to the caller, since we now know we were the leader when we executed the query.
  #     - if the role of the leader changes before the query's timeout (CheckQuorum triggered, or the leader is deposed), we return an error to the caller
  #     - otherwise, the query will time out
  #
  @impl true
  def handle_call({:query, :linearizable, query}, from, %State{role: :leader, waiting_for_first_commit: waiting} = state) when waiting != false do
    Logger.debug("rejecting linearizable query, not ready", logger_metadata(trace: {:query, :linearizable, :rejected_not_ready, from, query}))

    {:reply, {:error, :not_ready_for_queries}, state}
  end

  def handle_call({:query, :linearizable, query}, from, %State{role: :leader, global_clock: global_clock} = state) when not is_nil(global_clock) do
    if MemberCache.holding_lease?(state.name) do
      # write-optimized machine has unapplied commands
      state = apply_outstanding_entries(state)

      Logger.debug("executing query", logger_metadata(trace: {:query, :linearizable, :lease_read, from, query}))

      time(fn ->
        case state.module.handle_query(query, {:direct, from}, state.private) do
          {:reply, reply} ->
            {:reply, reply, state}

          :noreply ->
            {:noreply, state}
        end
      end,
      [:craft, :machine, :user, :handle_query],
      %{lease_read: true, linearizable: true})
    else
      {:reply, {:error, :not_leaseholder}, state}
    end
  end

  def handle_call({:query, :linearizable, query}, from, %State{role: :leader} = state) do
    # write-optimized machine has unapplied commands
    state = apply_outstanding_entries(state)

    Logger.debug("executing query", logger_metadata(trace: {:query, :linearizable, :quorum_read, from, query}))

    query_time = :erlang.monotonic_time(:millisecond)

    state =
      time(fn ->
        case state.module.handle_query(query, {:quorum, query_time, self(), from}, state.private) do
          {:reply, reply} ->
            %{state | client_query_results: :gb_trees.enter(query_time, {from, reply}, state.client_query_results)}

          :noreply ->
            %{state | pending_parallel_queries: MapSet.put(state.pending_parallel_queries, from)}
        end
      end,
      [:craft, :machine, :user, :handle_query],
      %{quorum_read: true, linearizable: true})

    {:noreply, state}
  end

  # only the leader can process linearizable queries
  def handle_call({:query, :linearizable, _query}, _from, state) do
    case MemberCache.get(state.name) do
      {:ok, %GroupStatus{leader: leader}} when not is_nil(leader) ->
        {:reply, {:error, {:not_leader, leader}}, state}

      _ ->
        {:reply, {:error, :unknown_leader}, state}
    end
  end

  # TODO: if a client directs a query to this node, which happens to be the leader, and it can't get quorum during a leaseless query (since we're just handling it as a leader query),
  #       we should redo again after it becomes a follower, instead of returning a "not leader" error.
  def handle_call({:query, :linearizable, :follower, query}, from, %State{role: :leader} = state) do
    Logger.debug("executing query", logger_metadata(trace: {:query, :linearizable, :follower, :read_index, from, query}))

    handle_call({:query, :linearizable, query}, from, state)
  end

  def handle_call({:query, :linearizable, :follower, query}, from, state) do
    Logger.debug("executing query", logger_metadata(trace: {:query, :linearizable, :follower, :read_index, from, query}))

    # avoids copying `state` into the task
    name = state.name
    read_index_task =
      Task.async(fn ->
        Craft.Raft.with_leader_redirect(name, fn node ->
          Craft.Raft.call_machine(name, node, :get_last_applied_index, 5_000)
        end)
      end)

    # write-optimized machine has unapplied commands
    state = apply_outstanding_entries(state)

    state = %{state | read_index_tasks: Map.put(state.read_index_tasks, read_index_task.ref, {from, query})}

    # continued in handle_info/2 when Task returns

    {:noreply, state}
  end

  def handle_call({:query, {:eventual, :leader}, query}, from, state) do
    if state.role == :leader do
      Logger.debug("executing query", logger_metadata(trace: {:query, :leader_eventual, from, query}))

      time(fn ->
        case state.module.handle_query(query, {:direct, from}, state.private) do
          {:reply, reply} ->
            {:reply, reply, state}

          :noreply ->
            {:noreply, state}
        end
      end,
      [:craft, :machine, :user, :handle_query],
      %{eventual: true})
    else
      case MemberCache.get(state.name) do
        {:ok, %GroupStatus{leader: leader}} when not is_nil(leader) ->
          {:reply, {:error, {:not_leader, leader}}, state}

        _ ->
          {:reply, {:error, :unknown_leader}, state}
      end
    end
  end

  def handle_call({:query, :eventual, query}, from, state) do
    Logger.debug("executing query", logger_metadata(trace: {:query, :eventual, :quorum_read, from, query}))

    time(fn ->
      case state.module.handle_query(query, {:direct, from}, state.private) do
        {:reply, reply} ->
          {:reply, reply, state}

        :noreply ->
          {:noreply, state}
      end
    end,
    [:craft, :machine, :user, :handle_query],
    %{eventual: true})
  end

  def handle_call({{:query_reply, query_time, reply}, query_from}, _from, state) do
    is_pending? = MapSet.member?(state.pending_parallel_queries, query_from)
    quorum_happend? = !is_nil(state.last_quorum_at) and query_time <= state.last_quorum_at

    if is_pending? do
      if quorum_happend? do
        GenServer.reply(query_from, reply)

        {:reply, :ok, %{state | pending_parallel_queries: MapSet.delete(state.pending_parallel_queries, query_from)}}
      else
        {:reply, :ok,
         %{
           state
           | pending_parallel_queries: MapSet.delete(state.pending_parallel_queries, query_from),
             client_query_results: :gb_trees.enter(query_time, {query_from, reply}, state.client_query_results)
         }}
      end
    else
      {:reply, :noop, state}
    end
  end

  # since it's over an rpc, `from` is the rpc process, and `async_caller` is the client (if it's an async command)
  def handle_call({:command, command, async_caller}, from, state) do
    case Consensus.command(state.name, command) do
      {:ok, index} ->
        Logger.debug("sent command to consensus", logger_metadata(trace: {:command, from, command, async_caller}))

        state = %{state | client_commands: Map.put(state.client_commands, index, {from, async_caller})}

        if async_caller do
          {_pid, ref} = from

          {:reply, {:ok, ref}, state}
        else
          {:noreply, state}
        end

      # not leader, not leaseholder, etc...
      error ->
        {:reply, error, state}
    end
  end

  def handle_call({:init_or_restore, log}, _from, state) do
    {last_applied, private, snapshot} =
      if state.module.__craft_mutable__() do
        data_dir =
          state.name
          |> Configuration.find()
          |> Map.fetch!(:data_dir)

        data_dir = Path.join([Configuration.data_dir(), data_dir, "machine"])

        File.mkdir_p!(data_dir)

        {:ok, private} = state.module.init(%{name: state.name, data_dir: data_dir})
        last_applied = state.module.last_applied_log_index(private)

        snapshot =
          case Persistence.first(log) do
            {keep_index, %SnapshotEntry{}} ->
              snapshots = state.module.snapshots(private)

              (Map.keys(snapshots) -- [keep_index])
              |> Enum.each(fn index ->
                Configuration.data_dir()
                |> Path.join(snapshots[index])
                |> File.rm_rf()
              end)

              {keep_index, snapshot_info(snapshots[keep_index])}

            _ ->
              nil
          end

        {last_applied, private, snapshot}
      else
        case Persistence.first(log) do
          {index, %SnapshotEntry{} = snapshot} ->
            {index, snapshot.machine_private, nil}

          _ ->
            {:ok, private} = state.module.init(state.name)

            {0, private, nil}
        end
      end

    me = self()
    # avoids passing `state` into the cleaner-upper process
    module = state.module
    spawn_link(fn ->
      Process.flag(:trap_exit, true)

      receive do
        {:EXIT, ^me, _reason} -> 
          if function_exported?(module, :close, 1) do
            module.close(private)
          end

        msg ->
          Logger.warning("machine cleaner-upper ignored an unknown message: #{inspect msg}")
      end
    end)

    {:reply, {:ok, snapshot, last_applied}, %{state | last_applied: last_applied, apply_up_to: last_applied, private: private}}
  end

  # delete on-disk machine files etc...
  def handle_call(:prepare_to_receive_snapshot, _from, state) do
    Logger.debug("preparing to receive snapshot", logger_metadata(trace: :preparing_to_receive_snapshot))

    {:ok, data_dir, private} = state.module.prepare_to_receive_snapshot(state.private)

    {:reply, {:ok, data_dir}, %{state | private: private}}
  end

  def handle_call({:receive_snapshot, install_snapshot}, _from, state) do
    Logger.debug("receiving snapshot", logger_metadata(trace: {:receiving_snapshot, install_snapshot}))

    {private, last_applied} =
      if state.module.__craft_mutable__() do
        private = state.module.receive_snapshot(state.private)
        last_applied = state.module.last_applied_log_index(private)

        {private, last_applied}
      else
        private = state.module.receive_snapshot(install_snapshot.log_entry.machine_private, state.private)
        last_applied = install_snapshot.log_index

        {private, last_applied}
      end

    state = %{state | private: private, apply_up_to: last_applied} |> update_last_applied(last_applied)

    {:reply, :ok, state}
  end

  def handle_call({:backup, to_directory}, _from, state) do
    if state.module.__craft_mutable__() do
      {:reply, state.module.backup(to_directory, state.private), state}
    else
      # log-stored machines don't need to write a backup
      {:reply, :ok, state}
    end
  end

  def handle_call({:switch_mode, mode}, _from, state) do
    {:reply, :ok, %{state | mode: mode}}
  end

  def handle_call(:get_last_applied_index, _from, %State{role: :leader, global_clock: global_clock} = state) when not is_nil(global_clock) do
    if MemberCache.holding_lease?(state.name) do
      # follower is about to service a read-index query, which is an externally visible event covered by consistency guarantees,
      # so we need to fast-forward the log if we're in write-optimized mode with unapplied commits, just as if it was serviced locally
      state = apply_outstanding_entries(state)

      {:reply, {:ok, state.last_applied}, state}
    else
      {:reply, {:error, :not_leaseholder}, state}
    end
  end

  def handle_call(:get_last_applied_index, from, %State{role: :leader} = state) do
    state = apply_outstanding_entries(state)

    query_time = :erlang.monotonic_time(:millisecond)
    state = %{state | client_query_results: :gb_trees.enter(query_time, {from, {:ok, state.last_applied}}, state.client_query_results)}

    {:noreply, state}
  end

  def handle_call(:get_last_applied_index, _from, state) do
    case MemberCache.get(state.name) do
      {:ok, %GroupStatus{leader: leader}} when not is_nil(leader) ->
        {:reply, {:error, {:not_leader, leader}}, state}

      _ ->
        {:reply, {:error, :unknown_leader}, state}
    end
  end

  @impl true
  def handle_call(:state, _from, state) do
    machine_state =
      if function_exported?(state.module, :dump, 1) do
        state.module.dump(state.private)
      else
        {:not_implemented, {state.module, {:dump, 1}}}
      end

    {:reply, %{state: state, machine_state: machine_state}, state}
  end

  @impl true
  def handle_info({ref, leader_last_applied_index_reply}, %State{read_index_tasks: read_index_tasks} = state) when is_map_key(read_index_tasks, ref) do
    receive do
      {:DOWN, ^ref, :process, _pid, :normal} -> :to_consume_down_message
    end

    {from, query} = Map.fetch!(read_index_tasks, ref)
    # our last_applied may have incremented beyond the result returned by the leader while we were waiting, this is safe, because our last_applied will
    # never exceed the leader's current last_applied (which is delivered via heartbeat).
    state =
      case leader_last_applied_index_reply do
        {:ok, leader_last_applied_index} ->
          if state.last_applied >= leader_last_applied_index do
            time(fn ->
              case state.module.handle_query(query, {:direct, from}, state.private) do
                {:reply, reply} ->
                  Logger.debug("immediately executing read-index query", logger_metadata(trace: {:sending_read_index_response, %{from: from, query: query, result: reply}}))
                  GenServer.reply(from, reply)

                :noreply ->
                  :noop
              end
            end,
            [:craft, :machine, :user, :handle_query],
            %{linearizable: true, follower_read: true})

            state
          else
            Logger.debug("enqueueing read-index query", logger_metadata(trace: {:enqueuing_read_index_response, %{from: from, query: query, last_applied: state.last_applied, wait_until_index: leader_last_applied_index}}))
            # enqueue query to run when we see the read-index
            queries_at_index =
              case :gb_trees.lookup(leader_last_applied_index, state.read_index_queries) do
                :none ->
                  MapSet.new([{from, query}])

                {:value, queries_at_index} ->
                  MapSet.put(queries_at_index, {from, query})
              end

            %{state | read_index_queries: :gb_trees.enter(leader_last_applied_index, queries_at_index, state.read_index_queries)}
          end

        {:error, _} = error ->
          GenServer.reply(from, error)

          state
      end

    {:noreply, %{state | read_index_tasks: Map.delete(state.read_index_tasks, ref)}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{read_index_tasks: read_index_tasks} = state) when is_map_key(read_index_tasks, ref) do
    {from, _query} = Map.fetch!(read_index_tasks, ref)

    GenServer.reply(from, {:error, reason})

    {:noreply, %{state | read_index_tasks: Map.delete(state.read_index_tasks, ref)}}
  end

  defp apply_outstanding_entries(state), do: apply_outstanding_entries(state, Consensus.get_log(state.name))
  defp apply_outstanding_entries(state, log) do
    range = state.last_applied+1..state.apply_up_to//1
    entries = Persistence.fetch_between(log, range)

    entries_with_index = Enum.zip(range, entries)

    apply_entries(state, entries_with_index)
  end

  defp apply_entries(state, []), do: state
  defp apply_entries(state, entries) do
    entries_by_type = Enum.group_by(entries, fn {_index, entry} -> entry.__struct__ end)

    state =
      Enum.reduce(entries_by_type[MembershipEntry] || [], state, fn {index, _entry}, state ->
        reply_to_command(state, index, :ok)
      end)

    command_entries = entries_by_type[CommandEntry] || []

    state =
      if command_entries != [] do
        if function_exported?(state.module, :handle_commands, 2) do
          Logger.debug("applying batch commands", logger_metadata(trace: {:applying_commands, command_entries}))

          commands_with_indexes = Enum.map(command_entries, fn {index, entry} -> {entry.command, index} end)
          {replies, private} =
            time(fn ->
              state.module.handle_commands(commands_with_indexes, state.private)
            end,
            [:craft, :machine, :user, :handle_commands],
            %{},
            %{num: Enum.count(commands_with_indexes)})

          state = %{state | private: private}

          # if the machine is write-optimized, we've already replied
          if state.mode != :write_optimized do
            command_entries
            |> Enum.zip(replies)
            |> Enum.reduce(state, fn {{index, _entry}, reply}, state ->
              reply_to_command(state, index, reply)
            end)
          else
            state
          end
        else
          Enum.reduce(command_entries, state, fn {index, entry}, state ->
            Logger.debug("applying command entry", logger_metadata(trace: {:applying_command, entry}))

            {reply, private} =
              time(fn ->
                case state.module.handle_command(entry.command, index, state.private) do
                  {reply, private} ->
                    {reply, private}

                    # {reply, side_effects, private} ->
                    #   if state.role == :leader do
                    #     Enum.each(side_effects, fn {m, f, a} ->
                    #       spawn(fn -> apply(m, f, a) end)
                    #     end)
                    #   end

                    #   {reply, private}
                end
              end,
              [:craft, :machine, :user, :handle_command],
              %{})


            state = %{state | private: private}

            if state.mode != :write_optimized do
              reply_to_command(state, index, reply)
            else
              state
            end
          end)
        end
      else
        state
      end
 
    {last_applied, _} = List.last(entries)

    update_last_applied(state, last_applied)
  end

  defp update_last_applied(%State{} = state, last_applied) do
    Consensus.set_last_applied(state.name, last_applied)

    %{state | last_applied: last_applied}
  end

  defp reply_to_command(state, index, reply) do
    with :leader <- state.role,
         {{from, async_caller}, client_commands} <- Map.pop(state.client_commands, index) do
      if async_caller do
        {_pid, ref} = from

        send(async_caller, {:"$craft_command", ref, reply})
      else
        GenServer.reply(from, reply)
      end

      %{state | client_commands: client_commands}
    else
      _ ->
        state
    end
  end

  @empty_gb_tree :gb_trees.empty()
  defp take_gb_tree(state, value, results \\ [])
  defp take_gb_tree(@empty_gb_tree, _value, results), do: {@empty_gb_tree, results}

  defp take_gb_tree(tree, value, results) do
    case :gb_trees.take_smallest(tree) do
      {this_value, result, new_tree} when this_value <= value ->
        take_gb_tree(new_tree, value, [result | results])

      _ ->
        {tree, results}
    end
  end

  defp snapshot_info(path) do
    files =
      path
      |> ls_flat()
      |> Enum.map(fn file ->
        %RemoteFile{
          name: Path.relative_to(file, path),
          md5: md5(file),
          byte_size: File.stat!(file).size
        }
      end)

    relative_path = Path.relative_to(path, Configuration.data_dir())

    {relative_path, files}
  end

  defp ls_flat(path) do
    path
    |> File.ls!()
    |> Enum.flat_map(fn entry ->
      entry = Path.join(path, entry)

      if File.dir?(entry) do
        ls_flat(entry)
      else
        [entry]
      end
    end)
  end

  defp md5(file) do
    file
    |> File.stream!(100_000)
    |> Enum.reduce(:erlang.md5_init(), &:erlang.md5_update(&2, &1))
    |> :erlang.md5_final()
  end

  defmacro __using__(opts) do
    mutable = !!Keyword.fetch!(opts, :mutable)

    quote do
      @behaviour Craft.Machine

      if unquote(mutable) do
        @behaviour MutableMachine
      else
        @behaviour LogStoredMachine
      end

      def __craft_mutable__(), do: unquote(mutable)
    end
  end
end
