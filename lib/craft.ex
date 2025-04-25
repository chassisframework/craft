#
# commands and queries are handled differently
#
# commands modify state, so they're automatically directed to the leader to go through a consensus round
#
# queries read state, their handling depends on the level of consistency desired:
#   - linearizable queries are always directed to the leader
#     - if leader leases are enabled (you're using an accurate time source), the leader's machine can immediately process the request
#     - otherwise, we need to wait for a round of consenus to occur to confirm that the leader is still the leader
#       - when a client makes a query, the machine process simply buffers it
#       - when the next consensus round achieves consensus, the consensus process informs the machine process of the (possibly bumped) commit index
#       - the machine process flushes the buffer and processes all waiting queries
#
#  - eventually consistent queries are immediately processed by the receiving node (random node or one provided by the user)
#    - if the user consistently uses the same node, monotonic read consistency is supported
#    - otherwise, if the user leaves it up to craft to pick a random node, there are no consistency guarantees, state can be arbitrarily stale, and even violate causality
#
defmodule Craft do
  alias Craft.Consensus
  alias Craft.Machine
  alias Craft.MemberCache
  alias Craft.Consensus.State.Members

  require Logger

  # FIXME
  @type group_name :: any()
  @type command :: any()
  @type reply :: any()
  @type side_effects :: any()
  @type query() :: any()
  @type log_index() :: non_neg_integer()

  @doc """
  Starts a raft group on a list of erlang nodes.

  `nodes` must be an accessible list of erlang nodes, and
  `machine` must be a module that uses `Craft.Machine` for functionality and implements its behaviour.

  ### Opts
    - `:persistence` - Configure how the WAL is persisted with a {module, args} tuple where module aheres to the `Craft.Persistence` behaviour
    - `:data_dir` - Customizes where rocksdb stores its WAL file.
  """
  def start_group(name, nodes, machine, opts \\ []) do
    for node <- nodes do
      :pong = Node.ping(node)
      {:module, __MODULE__} = :rpc.call(node, Code, :ensure_loaded, [__MODULE__])
    end

    for node <- nodes do
      {:ok, _pid} = :rpc.call(node, __MODULE__, :start_member, [name, nodes, machine, opts])
    end
  end

  @doc "Stops all members of the raft group"
  def stop_group(name) do
    with {:ok, %{members: members}} <- with_leader_redirect(name, &configuration(name, &1)) do
      results =
        members
        |> Members.all_nodes()
        |> Map.new(fn node ->
          result =
            try do
              :rpc.call(node, __MODULE__, :stop_member, [name])
            catch :exit, e ->
              e
            end

          {node, result}
        end)

      if Enum.all?(results, &match?({_, :ok}, &1)), do: :ok, else: results
    end
  end

  @doc """
  Adds an erlang node to an existing raft group.

  This function will also ensure the supervised processes for this raft group are
  running on the node to add before it tries to connect it.
  """
  def add_member(name, node) do
    :pong = Node.ping(node)

    {:ok, config} = with_leader_redirect(name, &configuration(name, &1))

    {%{
       members: members,
       machine_module: machine_module
     }, opts} = Map.split(config, [:members, :machine_module])

    for module <- [__MODULE__, machine_module] do
      {:module, ^module} = :rpc.call(node, Code, :ensure_loaded, [module])
    end

    # The nodes we provide to the new member here will eventually be overwritten when
    # the new member processes the MembershipEntry as it catches up to the leader.
    {:ok, _pid} = :rpc.call(node, __MODULE__, :start_member, [name, members.voting_nodes, machine_module, Keyword.new(opts)])

    with_leader_redirect(name, &Consensus.add_member(name, &1, node))
  end

  @doc "Removes a node from membership in a raft group, but doesn't stop its processes."
  def remove_member(name, node) do
    with_leader_redirect(name, &Consensus.remove_member(name, &1, node))
  end

  @doc "Selects a new node to become the leader of a raft group."
  def transfer_leadership(name, to_node) do
    with_leader_redirect(name, &Consensus.transfer_leadership(name, &1, to_node))
  end

  @doc "Starts the supervised processes for the named raft group on the node"
  defdelegate start_member(name, nodes, machine, opts), to: Craft.MemberSupervisor
  @doc "Stops the supervised processes for the named raft group on the node"
  defdelegate stop_member(name), to: Craft.MemberSupervisor
  @doc "Initializes the MemberCache for a raft group with the given nodes"
  defdelegate discover(name, nodes), to: Craft.MemberCache

  @doc """
  Commits a command onto the log and executes the `c:command/3` callback on the configured
  state machine set for the raft group.

  This does not return until it has been accepted by a quorum of nodes.

  ### Opts
  - `timeout` - (default: 5_000) the time before we return a timeout error
  """
  def command(command, name, opts \\ []) do
    with_leader_redirect(name, &Machine.command(name, &1, command, opts))
  end

  #
  # Craft.query(command, name, consistency: :linearizable)
  # if `consistency` is `:linearizable`, will address leader
  # if `consistency` is `:eventual`, will address random follower
  # if `consistency` is `{:eventual, node}`, will address given node
  #
  # Craft.command(command, name) # always goes to leader, since it can modify state
  #
  # TODO: timeout in opts
  #       let user choose if query handler in Machine.handle_cast(:query) spawns off a new process, to unblock the machine
  #         - it'll copy the machine's state to a new process, which could be worth it in some scenarios
  #

  @doc """
  Runs a read-only query against the machine state without committing a log message.

  Depending on the `consistency` option's value, queries can access other nodes than the leader to
  spread load. This is in contrast to a query that always addresses the leader since it can modify state.

  ### Opts
  - `:consistency` - Configures the type of consistency guarentees for the query
  - `timeout` - (default: 5_000) the time before we return a timeout error

  ### Consistency values
  - `:linearizable` - query is run on the leader
  - `:eventual` - query is run on a random node
  - `{:eventual, :leader}` - query is run on the leader without checking for quorum before returning result
  - `{:eventual, {:node, node}}` - query is run on the given node without linearizable guarentees
  """
  def query(query, name, opts \\ []) do
    {consistency, opts} = Keyword.pop(opts, :consistency, :linearizable)

    case consistency do
      :linearizable ->
        with_leader_redirect(name, &Machine.query(name, &1, query, consistency, opts))

      {:eventual, :leader} ->
        with_leader_redirect(name, &Machine.query(name, &1, query, :eventual, opts))

      {:eventual, {:node, node}} ->
        Machine.query(name, node, query, :eventual, opts)

      :eventual ->
        case MemberCache.get(name) do
          {:ok, _leader, members} ->
            node = Enum.random(members)

            Machine.query(name, node, query, consistency, opts)

          :not_found ->
            Logger.error("No known nodes for group '#{inspect(name)}', have you called Craft.discover/2?")

            {:error, :unknown_group}
        end
    end
  end

  @doc "Requests a different leader than the current."
  def step_down(name, node) do
    :gen_statem.cast({Consensus.name(name), node}, :step_down)
  end

  defp with_leader_redirect(name, func) do
    case MemberCache.get(name) do
      {:ok, nil, members} ->
        do_leader_redirect(name, Enum.random(members), members, func)

      {:ok, leader, members} ->
        do_leader_redirect(name, leader, members, func)

      :not_found ->
        Logger.error("No known nodes for group '#{inspect(name)}', have you called Craft.discover/2?")

        {:error, :unknown_group}
    end
  end

  defp do_leader_redirect(name, leader, members, func, previous_redirects \\ MapSet.new()) do
    case func.(leader) do
      {:error, :unknown_leader} ->
        members = MapSet.delete(members, leader)

        if Enum.empty?(members) do
          {:error, :unknown_leader}
        else
          do_leader_redirect(name, Enum.random(members), members, func)
        end

      {:error, {:not_leader, leader}} ->
        if MapSet.member?(previous_redirects, leader) do
          {:error, :redirect_loop}
        else
          MemberCache.update_leader(name, leader)

          do_leader_redirect(name, leader, members, func, previous_redirects)
        end

      reply ->
        reply
    end
  end

  @doc false
  def state(name, node) do
    try do
      {node,
       consensus: Consensus.state(name, node),
       machine: Machine.state(name, node)}
    catch :exit, e ->
      {node, e}
    end
  end

  @doc false
  def state(name) do
    {:ok, %{members: members}} = with_leader_redirect(name, &configuration(name, &1))

    members.voting_nodes
    |> MapSet.union(members.non_voting_nodes)
    |> Enum.into(%{}, &state(name, &1))
  end

  defp configuration(name, node) do
    try do
      Consensus.configuration(name, node)
    catch :exit, e ->
      {:error, e}
    end
  end
end
