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

  # FIXME
  @type group_name :: any()
  @type command :: any()
  @type reply :: any()
  @type side_effects :: any()
  @type query() :: any()
  @type log_index() :: non_neg_integer()

  case Application.compile_env(:craft, :backend, __MODULE__.Raft) do
    {module, _args} ->
      def backend, do: unquote(module)

    module ->
      def backend, do: unquote(module)
  end

  @doc """
  Starts a raft group on a list of erlang nodes.

  `nodes` must be an accessible list of erlang nodes, and
  `machine` must be a module that uses `Craft.Machine` for functionality and implements its behaviour.

  ### Opts
    - `:persistence` - Configure how the log is persisted with a {module, args} tuple where module aheres to the `Craft.Persistence` behaviour
  """
  def start_group(name, nodes, machine, opts \\ []), do: backend().start_group(name, nodes, machine, opts)

  @doc "Stops all members of the group"
  def stop_group(name), do: backend().stop_group(name)

  @doc "Starts the local member with the given name"
  def start_member(name, opts \\ []), do: backend().start_member(name, opts)

  @doc "Stops the local member with the given name"
  def stop_member(name), do: backend().stop_member(name)

  @doc """
  Adds a node to an existing group.

  ### Opts
  - `timeout` - (default: 5_000) the time before we return a timeout error
  """
  def add_member(name, node, opts \\ []), do: backend().add_member(name, node, opts)

  @doc """
  Removes a node from membership in the group, but doesn't stop its processes.

  ### Opts
  - `timeout` - (default: 5_000) the time before we return a timeout error
  """
  def remove_member(name, node, opts \\ []), do: backend().remove_member(name, node, opts)

  @doc "Selects a new node to become the leader of the group."
  def transfer_leadership(name, to_node), do: backend().transfer_leadership(name, to_node)

  @doc "Transfers leadership to a random follower."
  def transfer_leadership(name), do: backend().transfer_leadership(name)

  @doc "Writes a backup of the local member of the given group to the provided path."
  def backup(name, path), do: backend().backup(name, path)

  @doc "Copies the backup at the given path to Craft's data directory. You must call Craft.start_member/1 afterwards."
  def restore(path), do: backend().restore(path)

  @doc """
  Sends a message to the local instance of the user's state machine.

  Receivable by the `handle_info/2` callback.
  """
  def send(name, message), do: backend().send(name, message)

  @doc "Responds to an asynchronous query, akin to GenServer.reply/2"
  def reply(meta, reply), do: backend().reply(meta, reply)

  @doc "Initializes the MemberCache for the group with the given nodes"
  def discover(name, nodes), do: backend().discover(name, nodes)

  @doc """
  Indicates, for the given group, if this node:
    1. holds the lease, and
    2. the user's machine is 'caught up' after an election

  This function is intended to extend craft's linearizabiliy guarantees to processes outside of the user's state machine.
  """
  def holding_lease?(name), do: backend().holding_lease?(name)

  @doc "See holding_lease?/1, called from within a user's machine callback"
  def holding_lease?(), do: backend().holding_lease?()

  @doc "Lists the groups known to this node, with cached information (members, lease holder, etc..)"
  def known_groups(), do: backend().known_groups()

  @doc "Lists cached information about the given group."
  def cached_info(group_name), do: backend().cached_info(group_name)

  @doc "Destroys all local data for the given group."
  def purge(name), do: backend().purge(name)

  @doc "Gives the current time, if the machine has a global clock configured. Called from within the user's machine callback."
  def now(), do: backend().now()

  @doc """
  Submits a command to the given group `name`, once quorum is reached, the command is executed.

  See `c:Machine.handle_command/3`.

  ### Opts
  - `timeout` - (default: 5_000) the time before we return a timeout error
  """
  def command(command, name, opts \\ []), do: backend().command(command, name, opts)

  #
  # Craft.query(command, name, consistency: :linearizable)
  # if `consistency` is `:linearizable`, will address leader
  # if `consistency` is `:eventual`, will address random follower
  # if `consistency` is `{:eventual, node}`, will address given node
  #
  # Craft.command(command, name) # always goes to leader, since it can modify state

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
  def query(query, name, opts \\ []), do: backend().query(query, name, opts)

  @doc "Requests a different leader than the current."
  def step_down(name), do: backend().step_down(name)

  @doc false
  def state(name, node), do: backend().state(name, node)

  @doc false
  def state(name), do: backend().state(name)
end
