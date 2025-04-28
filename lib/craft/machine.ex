defmodule Craft.Machine do
  @doc false
  use GenServer

  alias Craft.Consensus
  alias Craft.Consensus.State, as: ConsensusState
  alias Craft.Consensus.State.LeaderState
  alias Craft.Persistence
  alias Craft.Log.CommandEntry
  alias Craft.Log.EmptyEntry
  alias Craft.Log.MembershipEntry
  alias Craft.Log.SnapshotEntry

  import Craft.Application, only: [via: 2, lookup: 2]

  @type private :: any()
  @type snapshot :: any()

  @callback init(Craft.group_name()) :: {:ok, private()}
  @callback command(Craft.command(), Craft.log_index(), private()) :: {Craft.reply(), private()} | {Craft.reply(), Craft.side_effects(), private()}
  @callback query(Craft.query(), private()) :: Craft.reply()
  # TODO: document that we want an int or nil if no entries have been applied

  defmodule MutableMachine do
    @type private() :: Craft.Machine.private()
    @type snapshot() :: Craft.Machine.snapshot()
    @type data_dir :: Path.t()

    @callback last_applied_log_index(private()) :: Craft.log_index() | nil
    @callback snapshot(Craft.log_index(), private()) :: {:ok, snapshot()}
    @callback prepare_to_receive_snapshot(private()) :: {:ok, data_dir, snapshot()}
    @callback receive_snapshot(private()) :: {:ok, private()}
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
      last_applied: 0,
      client_queries: :gb_trees.empty(),
      client_commands: %{}
    ]

    def pop_query_results_earlier_than(%__MODULE__{} = state, time) do
      all_results =
        time
        |> :gb_trees.iterator_from(state.client_queries, :reversed)
        |> do_pop_earlier_than()

      {client_queries, results} =
        Enum.reduce(all_results, {state.client_queries, MapSet.new()}, fn {key, results}, {tree, all_results} ->
          {:gb_trees.delete(key, tree), MapSet.union(results, all_results)}
        end)

      {results, %State{state | client_queries: client_queries}}
    end

    defp do_pop_earlier_than(iterator, all_results \\ MapSet.new()) do
      case :gb_trees.next(iterator) do
        :none ->
          all_results

        {key, results, iterator} ->
          do_pop_earlier_than(iterator, MapSet.put(all_results, {key, results}))
      end
    end
  end

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: via(args.name, __MODULE__))
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
  def commit_index_bumped(%ConsensusState{} = state, should_snapshot?) do
    state.name
    |> lookup(__MODULE__)
    |> GenServer.cast({:commit_index_bumped, state.commit_index, state.persistence, state.state, should_snapshot?})
  end

  def quorum_reached(%ConsensusState{leader_state: %LeaderState{last_quorum_at: last_quorum_at}} = state) when is_integer(last_quorum_at) do
    state.name
    |> lookup(__MODULE__)
    |> GenServer.cast({:quorum_reached, last_quorum_at})
  end
  def quorum_reached(_state), do: :noop

  def command(name, node, command, opts \\ []) do
    request(name, node, :command, command, opts)
  end

  def query(name, node, query, consistency, opts \\ []) do
    request(name, node, {:query, consistency}, query, opts)
  end

  defp request(name, node, type, request, opts) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    id = {self(), make_ref()}

    with :ok <- :rpc.call(node, __MODULE__, :cast_request, [name, id, type, request]) do
      receive do
        {^id, reply} ->
          reply
      after
        timeout ->
          {:error, :timeout}
      end
    else e ->
        {:error, e}
    end
  end

  def cast_request(name, id, type, request) do
    name
    |> lookup(__MODULE__)
    |> GenServer.cast({type, id, request})
  end

  @impl true
  def init(args) do
    if nexus_pid = args[:nexus_pid] do
      remote_group_leader = :rpc.call(node(nexus_pid), Process, :whereis, [:init])
      :logger.update_process_metadata(%{gl: remote_group_leader})
    end

    {:ok, private} = args.machine.init(args.name)

    state =
      %State{
        name: args.name,
        module: args.machine,
        private: private
      }

    {:ok, state, {:continue, :restore_machine_state}}
  end

  @impl true
  def handle_continue(:restore_machine_state, state) do
    {commit_index, log} = Consensus.catch_up(state.name)

    handle_cast({:commit_index_bumped, commit_index, log, nil, false}, state)
  end

  @impl true
  def handle_cast({:update_role, role}, state) do
    # if we were just the leader and are holding in-flight requests, but we've been deposed, we need to let the awaiting clients know
    {:ok, leader, _members} = Craft.MemberCache.get(state.name)

    # if we're the just-deposed leader, we probably don't know who the new leader is
    response =
      if state.role == :leader && role != :leader && node() == leader do
        {:error, :unknown_leader}
      else
        {:error, {:not_leader, leader}}
      end

    for {{_ref, from} = id, _result} <- state.client_queries |> :gb_trees.to_list() |> Keyword.values() do
      send(from, {id, response})
    end

    for {_index, {from, _ref} = id} <- state.client_commands do
      send(from, {id, response})
    end

    {:noreply, %State{state | role: role, client_commands: %{}, client_queries: :gb_trees.empty()}}
  end

  # when a consensus round completes, the consensus process sends us the last time at which we were guaranteed to be the leader
  def handle_cast({:quorum_reached, time}, state) do
    if :gb_trees.is_empty(state.client_queries) do
      {:noreply, state}
    else
      {results, state} = State.pop_query_results_earlier_than(state, time)

      for {{from, _ref} = id, result} <- results do
        send(from, {id, result})
      end

      {:noreply, state}
    end
  end

  @impl true
  def handle_cast({:commit_index_bumped, new_commit_index, log, role, should_snapshot?}, state) do
    last_applied_log_index =
      with true <- state.module.__craft_mutable__(),
           last_applied when is_integer(last_applied) <- state.module.last_applied_log_index(state.private) do
        last_applied
      else
        _ ->
          state.last_applied
      end

    # right now, this is a synchronous process, later we should allow for async snapshots at
    # the provided index, and the user will call back when it's done (and we'll send a message
    # to the consensus module to tell it that a snapshot at the given index completed)
    #
    # should probably provide sync/async semantics as well as ability to store small snapshots directly in the log
    #
    # only snapshot up to one entry before the latest, since we need the prev log entry to create AppendEntries

    if should_snapshot? do
      if state.module.__craft_mutable__() do
        snapshot_index = new_commit_index - 1
        snapshot_dir = state.module.snapshot(snapshot_index, state.private)

        :ok = Consensus.snapshot_ready(state.name, snapshot_index, snapshot_dir)
      else
        snapshot_index = last_applied_log_index
        snapshot_content = state.private

        :ok = Consensus.snapshot_ready(state.name, snapshot_index, snapshot_content)
      end
    end

    state =
      Enum.reduce(last_applied_log_index+1..new_commit_index//1, state, fn index, state ->
        case Persistence.fetch(log, index) do
          {:ok, %SnapshotEntry{} = entry} ->
            if state.module.__craft_mutable__() do
              state
            else
              %State{state | private: entry.machine_private}
            end

          {:ok, %EmptyEntry{}} ->
            state

          {:ok, %MembershipEntry{}} ->
            state

          {:ok, %CommandEntry{command: command}} ->
            {reply, side_effects, private} =
              case state.module.command(command, index, state.private) do
                {reply, private} ->
                  {reply, [], private}

                {reply, side_effects, private} ->
                  {reply, side_effects, private}
              end

            state = %State{state | private: private}

            if role == :leader do
              Enum.each(side_effects, fn {m, f, a} ->
                spawn(fn -> apply(m, f, a) end)
              end)

              case Map.pop(state.client_commands, index) do
                {{pid, _ref} = id, client_commands} ->
                  send(pid, {id, reply})

                  %State{state | client_commands: client_commands}

                _ ->
                  state
              end
            else
              state
            end
        end
      end)

    {:noreply, %State{state | last_applied: new_commit_index}}
  end

  #
  # the leader handles linearizable queries slightly differently, depending on if leases are enabled:
  #   - if leases are enabled,
  #       - and we're within the lease period, the leader knows that it is the current leader, so it immediately processes a query
  #       - and we're outside the lease period, we fall back to quorum-seeking
  #   - if leases are disabled, we need to see a quorum round succeed to know that we were still the leader when the query was executed. so,
  #       we execute the query speculatively and store the result along with the current monotonic time, all the while, the consensus process
  #       is streaming monotonic timestamps of when the most recent quorum was achieved.
  #     - when the quorum timestamp exceeds the query's timestamp, we send the result to the caller, since we now know we were the leader when we executed the query.
  #     - if the role of the leader changes before the query's timeout (CheckQuorum triggered, or the leader is deposed), we return an error to the caller
  #     - otherwise, the query will time out
  #
  @impl true
  def handle_cast({{:query, :linearizable}, id, query}, %State{role: :leader} = state) do
    result = state.module.query(query, state.private)

    now = :erlang.monotonic_time(:millisecond)
    client_queries =
      case :gb_trees.lookup(now, state.client_queries) do
        {:value, queries} ->
          :gb_trees.update(now, MapSet.put(queries, {id, result}), state.client_queries)

        :none ->
          :gb_trees.insert(now, MapSet.new([{id, result}]), state.client_queries)
      end

    {:noreply, %State{state | client_queries: client_queries}}
  end

  # only the leader can process linearizable queries
  def handle_cast({{:query, :linearizable}, {from, _ref} = id, _query}, state) do
    {:ok, leader, _members} = Craft.MemberCache.get(state.name)

    send(from, {id, {:error, {:not_leader, leader}}})

    {:noreply, state}
  end

  def handle_cast({{:query, {:eventual, :leader}}, {from, _ref} = id, query}, state) do
    if state.role == :leader do
      send(from, {id, state.module.query(query, state.private)})
    else
      case Craft.MemberCache.get(state.name) do
        {:ok, leader, _members} -> send(from, {id, {:error, {:not_leader, leader}}})
        :not_found -> send(from, {id, {:error, :unknown_leader}})
      end
    end

    {:noreply, state}
  end

  def handle_cast({{:query, :eventual}, {from, _ref} = id, query}, state) do
    result = state.module.query(query, state.private)

    send(from, {id, result})

    {:noreply, state}
  end

  def handle_cast({:command, {from, _ref} = id, command}, state) do
    case Consensus.command(state.name, command) do
      {:ok, index} ->
        {:noreply, %State{state | client_commands: Map.put(state.client_commands, index, id)}}

      # not leader etc
      error ->
        send(from, {id, error})

        {:noreply, state}
    end
  end

  # delete on-disk machine files etc...
  @impl true
  def handle_call(:prepare_to_receive_snapshot, _from, state) do
    {:ok, data_dir, private} = state.module.prepare_to_receive_snapshot(state.private)

    {:reply, {:ok, data_dir}, %State{state | private: private}}
  end

  @impl true
  def handle_call({:receive_snapshot, install_snapshot}, _from, state) do
    {private, last_applied} =
      if state.module.__craft_mutable__() do
        private = state.module.receive_snapshot(state.private)
        last_applied = state.module.last_applied_log_index(state.private)

        {private, last_applied}
      else
        private = state.module.receive_snapshot(install_snapshot.log_entry.machine_private, state.private)
        last_applied = install_snapshot.log_index

        {private, last_applied}
      end

    {:reply, :ok, %State{state | private: private, last_applied: last_applied}}
  end

  @impl true
  def handle_call(:state, _from, state) do
    machine_state = state.module.dump(state.private)

    {:reply, %{state: state, machine_state: machine_state}, state}
  end

  defmacro __using__(opts) do
    mutable = !!Keyword.fetch!(opts, :mutable)

    quote do
      if unquote(mutable) do
        @behaviour MutableMachine
      else
        @behaviour LogStoredMachine
      end
      def __craft_mutable__(), do: unquote(mutable)
    end
  end
end
