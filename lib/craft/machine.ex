defmodule Craft.Machine do
  @doc false
  use GenServer

  alias Craft.Configuration
  alias Craft.Consensus
  alias Craft.Consensus.State, as: ConsensusState
  alias Craft.GlobalTimestamp
  alias Craft.Log.CommandEntry
  alias Craft.Log.EmptyEntry
  alias Craft.Log.MembershipEntry
  alias Craft.Log.SnapshotEntry
  alias Craft.MemberCache
  alias Craft.Persistence
  alias Craft.SnapshotServer.RemoteFile

  import Craft.Tracing, only: [logger_metadata: 1]
  import Craft.Application, only: [via: 2, lookup: 2]

  require Logger

  @type private :: any()
  @type snapshot :: any()
  @type role :: :receiving_snapshot | :lonely | :follower | :candidate | :leader
  @type reply_from :: {:direct, GenServer.from()} | {:quorum, query_time :: integer(), pid(), GenServer.from()}

  @callback init(Craft.group_name()) :: {:ok, private()}
  @callback handle_command(Craft.command(), Craft.log_index(), private()) :: {Craft.reply(), private()} | {Craft.reply(), Craft.side_effects(), private()}
  @callback handle_query(Craft.query(), reply_from(), private()) :: {:reply, Craft.reply()} | :noreply
  @callback handle_role_change(role(), private()) :: private()
  @callback handle_info(term(), private()) :: private()
  @optional_callbacks handle_role_change: 2, handle_info: 2

  defmodule MutableMachine do
    @type private() :: Craft.Machine.private()
    @type snapshot() :: Craft.Machine.snapshot()
    @type data_dir() :: Path.t()
    @type index() :: pos_integer()

    @callback last_applied_log_index(private()) :: Craft.log_index() | nil
    @callback snapshot(private()) :: {index(), snapshot(), private()} | nil
    @callback snapshots(private()) :: %{index() => snapshot()}
    @callback prepare_to_receive_snapshot(private()) :: {:ok, data_dir(), private()}
    @callback receive_snapshot(private()) :: {:ok, private()}
    @callback backup(to_directory :: Path.t(), private()) :: :ok | {:error, any()}
  end

  defmodule LogStoredMachine do
    @type private() :: Craft.Machine.private()
    @type snapshot() :: Craft.Machine.snapshot()

    @callback snapshot(private()) :: {:ok, snapshot()}
    @callback receive_snapshot(snapshot(), private()) :: {:ok, private()}
  end

  defmodule State do
    defstruct [
      :name,
      :module,
      :private,
      :role,
      :global_clock,
      :lease_expires_at,
      :last_quorum_at,
      last_applied: 0,
      client_query_results: [],
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

  def update_role(%ConsensusState{} = state) do
    state.name
    |> lookup(__MODULE__)
    |> GenServer.cast({:update_role, state.state})
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
  def quorum_reached(%ConsensusState{} = state, should_snapshot?) do
    lease_expires_at =
      if state.global_clock && state.state == :leader && !state.leader_state.waiting_for_lease do
        state.lease_expires_at
      end

    last_quorum_at = get_in(state.leader_state.last_quorum_at)

    state.name
    |> lookup(__MODULE__)
    |> GenServer.cast({:quorum_reached, state.commit_index, state.persistence, should_snapshot?, lease_expires_at, last_quorum_at})
  end

  def call(name, node, request, timeout) do
    :rpc.call(node, __MODULE__, :do_call, [name, request, timeout])
  end

  def do_call(name, request, timeout) do
    name
    |> lookup(__MODULE__)
    |> GenServer.call(request, timeout)
  end

  @doc "Allows responding to a query from a different process, like GenServer.reply"
  def reply({:direct, query_from}, reply) do
    GenServer.reply(query_from, reply)
  end

  def reply({:quorum, query_time, machine_pid, query_from}, reply) do
    GenServer.call(machine_pid, {{:query_reply, query_time, reply}, query_from})
  end

  @impl true
  def init(args) do
    Logger.metadata(name: args.name, node: node(), nexus: args[:nexus_pid])

    if nexus_pid = args[:nexus_pid] do
      remote_group_leader = :rpc.call(node(nexus_pid), Process, :whereis, [:init])
      :logger.update_process_metadata(%{gl: remote_group_leader})
    end

    {:ok, %State{name: args.name, module: args.machine, global_clock: args[:global_clock]}}
  end

  @impl true
  def handle_cast({:update_role, new_role}, state) do
    # if we were just the leader and are holding in-flight requests, but we've been deposed, we need to let the awaiting clients know
    {:ok, group_status} = MemberCache.get(state.name)

    # if we're the just-deposed leader, we probably don't know who the new leader is
    response =
      if state.role == :leader && node() == group_status.leader && new_role != :leader do
        {:error, :unknown_leader}
      else
        {:error, {:not_leader, group_status.leader}}
      end

    for {from, _result} <- state.client_query_results do
      GenServer.reply(from, response)
    end

    for {_index, from} <- state.client_commands do
      GenServer.reply(from, response)
    end

    for from <- state.pending_parallel_queries do
      GenServer.reply(from, response)
    end

    private =
      if function_exported?(state.module, :handle_role_change, 2) do
        state.module.handle_role_change(new_role, state.private)
      else
        state.private
      end

    {:noreply,
     %{
       state
       | role: new_role,
         private: private,
         client_commands: %{},
         client_query_results: [],
         pending_parallel_queries: MapSet.new()
     }}
  end

  @impl true
  def handle_cast({:quorum_reached, new_commit_index, log, should_snapshot?, lease_expires_at, last_quorum_at}, state) do
    Logger.debug(fn ->
      metadata = %{}
      metadata = if lease_expires_at, do: Map.put(metadata, :lease_expires_at, lease_expires_at), else: metadata
      metadata = if new_commit_index > state.last_applied, do: Map.put(metadata, :new_commit_index, new_commit_index), else: metadata

      {"quorum reached", logger_metadata(trace: {:quorum_reached, metadata})}
    end)

    state =
      if state.role == :leader do
        # read-index based queries
        for {from, result} <- state.client_query_results do
          Logger.debug("responding to client query", logger_metadata(trace: {:sending_client_response, %{from: from, result: result}}))
          GenServer.reply(from, result)
        end

        state = %{state | client_query_results: [], lease_expires_at: lease_expires_at}

        if lease_expires_at do
          MemberCache.update_lease_holder(state)
        end

        state
      else
        state
      end

    state =
      Enum.reduce(state.last_applied+1..new_commit_index//1, state, fn index, state ->
        case Persistence.fetch(log, index) do
          {:ok, %EmptyEntry{}} ->
            state

          {:ok, %MembershipEntry{}} ->
            reply_to_command(state, index, :ok)

          {:ok, %CommandEntry{command: command} = entry} ->
            Logger.debug("applying command entry", logger_metadata(trace: {:applying_command, entry}))

            {reply, private} =
              case state.module.handle_command(command, index, state.private) do
                {reply, private} ->
                  {reply, private}

                {reply, side_effects, private} ->
                  if state.role == :leader do
                    Enum.each(side_effects, fn {m, f, a} ->
                      spawn(fn -> apply(m, f, a) end)
                    end)
                  end

                  {reply, private}
              end

            reply_to_command(%{state | private: private}, index, reply)
        end
      end)

    #
    # right now, snapshotting is a synchronous process, later we should allow for async snapshots at
    # the provided index, and the user will call back when it's done (and we'll send a message
    # to the consensus module to tell it that a snapshot at the given index completed)
    #
    # should probably provide sync/async semantics as well
    #
    # only snapshot up to one entry before the latest, since we need the prev log entry to create AppendEntries
    #
    private =
      if should_snapshot? do
        if state.module.__craft_mutable__() do
          case state.module.snapshot(state.private) do
            {index, path, private} ->
              Logger.debug("snapshot ready", logger_metadata(trace: :snapshot_ready))

              :ok = Consensus.snapshot_ready(state.name, index, snapshot_info(path))

              private

            # machine decided not to snapshot (perhaps no commands have run)
            _ ->
              state.private
          end
        else
          Logger.debug("snapshot ready", logger_metadata(trace: :snapshot_ready))

          :ok = Consensus.snapshot_ready(state.name, state.last_applied, state.module.snapshot(state.private))

          state.private
        end
      else
        state.private
      end

    {:noreply, %{state | last_applied: new_commit_index, private: private, last_quorum_at: last_quorum_at}}
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
  def handle_call({:query, :linearizable, query}, from, %State{role: :leader, global_clock: global_clock} = state) when not is_nil(global_clock) do
    with %GlobalTimestamp{} <- state.lease_expires_at,
         {:ok, lease_time} when lease_time > 0 <- GlobalTimestamp.time_until_lease_expires(state.global_clock, state.lease_expires_at) do
      Logger.debug("executing query", logger_metadata(trace: {:query, :linearizable, :lease_read, from, query}))

      case state.module.handle_query(query, {:direct, from}, state.private) do
        {:reply, reply} -> {:reply, reply, state}
        :noreply -> {:noreply, state}
      end
    else
      _ ->
      {:reply, {:error, :not_leaseholder}, state}
    end
  end

  def handle_call({:query, :linearizable, query}, from, %State{role: :leader} = state) do
    Logger.debug("executing query", logger_metadata(trace: {:query, :linearizable, :quorum_read, from, query}))

    query_time = :erlang.monotonic_time(:millisecond)

    state = 
      case state.module.handle_query(query, {:quorum, query_time, self(), from}, state.private) do
        {:reply, reply} -> %{state | client_query_results: [{from, reply} | state.client_query_results]}
        :noreply -> %{state | pending_parallel_queries: MapSet.put(state.pending_parallel_queries, from)}
      end

    {:noreply, state}
  end

  # only the leader can process linearizable queries
  def handle_call({:query, :linearizable, _query}, _from, state) do
    {:ok, group_status} = MemberCache.get(state.name)

    {:reply, {:error, {:not_leader, group_status.leader}}, state}
  end

  def handle_call({:query, {:eventual, :leader}, query}, from, state) do
    if state.role == :leader do
      Logger.debug("executing query", logger_metadata(trace: {:query, :leader_eventual, :quorum_read, from, query}))

      case state.module.handle_query(query, {:direct, from}, state.private) do
        {:reply, reply} -> {:reply, reply, state}
        :noreply -> {:noreply, state}
      end
    else
      case MemberCache.get(state.name) do
        {:ok, group_status} ->
          {:reply, {:error, {:not_leader, group_status.leader}}, state}

        :not_found ->
          {:reply, {:error, :unknown_leader}, state}
      end
    end
  end

  def handle_call({:query, :eventual, query}, from, state) do
    Logger.debug("executing query", logger_metadata(trace: {:query, :eventual, :quorum_read, from, query}))

    case state.module.handle_query(query, {:direct, from}, state.private) do
      {:reply, reply} -> {:reply, reply, state}
      :noreply -> {:noreply, state}
    end
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
             client_query_results: [{query_from, reply} | state.client_query_results]
         }}
      end
    else
      {:reply, :noop, state}
    end
  end

  def handle_call({:command, command}, from, state) do
    case Consensus.command(state.name, command) do
      {:ok, index} ->
        Logger.debug("sent command to consensus", logger_metadata(trace: {:command, from, command}))

        {:noreply, %{state | client_commands: Map.put(state.client_commands, index, from)}}

      # not leader, not leaseholder, etc...
      error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_call({:init_or_restore, log}, _from, state) do
    {last_applied, private, snapshot} =
      if state.module.__craft_mutable__() do
        {:ok, private} = state.module.init(state.name)
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

    {:reply, {:ok, snapshot}, %{state | last_applied: last_applied, private: private}}
  end

  # delete on-disk machine files etc...
  @impl true
  def handle_call(:prepare_to_receive_snapshot, _from, state) do
    Logger.debug("preparing to receive snapshot", logger_metadata(trace: :preparing_to_receive_snapshot))

    {:ok, data_dir, private} = state.module.prepare_to_receive_snapshot(state.private)

    {:reply, {:ok, data_dir}, %{state | private: private}}
  end

  @impl true
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

    {:reply, :ok, %{state | private: private, last_applied: last_applied}}
  end

  @impl true
  def handle_call({:backup, to_directory}, _from, state) do
    {:reply, state.module.backup(to_directory, state.private), state}
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
  def handle_info(msg, state) do
    private =
      if function_exported?(state.module, :handle_info, 2) do
        state.module.handle_info(msg, state.private)
      else
        Logger.error("Message #{inspect(msg)} received but no handle_info defined in #{inspect(state.module)}")
        state.private
      end

    {:noreply, %{state | private: private}}
  end

  defp reply_to_command(state, index, reply) do
    with :leader <- state.role,
         {from, client_commands} when not is_nil(from) <- Map.pop(state.client_commands, index) do
      GenServer.reply(from, reply)

      %{state | client_commands: client_commands}

    else
      _ ->
        state
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
