defmodule Craft.Nexus do
  @moduledoc """
  this module starts a raft group, then directs all members to use it
  as the recipient for all cluster messages. it's a proxy to watch
  messages exchanged by the raft group as well as function trace messages
  emitted by individual members.
  """
  use GenServer

  alias Craft.Consensus
  # alias Craft.Consensus.FollowerState
  alias Craft.Consensus.CandidateState
  # alias Craft.Consensus.LeaderState
  alias Craft.RPC.AppendEntries

  defmodule State do
    defstruct [
      members: [],
      term: -1,
      leader: nil,
      empty_append_entries_counts: %{}, # num of consecutive successfully received and responded-to empty AppendEntries msgs
      genstatem_invocations: [],
      wait_until: {_watcher_from = nil, _condition = nil} # watcher termination condition (:group_stable, :millisecs, etc)
    ]

    def leader_elected(%State{term: term} = state, leader, new_term) when new_term == term + 1 do
      empty_append_entries_counts =
        state.members
        |> List.delete(leader)
        |> Enum.into(%{}, & {&1, {0, :confirmed}})

      %__MODULE__{state | empty_append_entries_counts: empty_append_entries_counts, leader: leader, term: new_term}
    end

    def append_entries_received(%State{term: term, leader: leader} = state, member, %AppendEntries{term: term, leader_id: leader} = append_entries) do
      empty_append_entries_counts =
        if append_entries.entries == [] do
          Map.update!(state.empty_append_entries_counts, member, fn {num, :confirmed} -> {num, :received} end)
        else
          Map.put(state.empty_append_entries_counts, member, {0, :confirmed})
        end

      %__MODULE__{state | empty_append_entries_counts: empty_append_entries_counts}
    end

    def append_entries_results_received(%State{term: term, leader: leader} = state, leader, %AppendEntries.Results{term: term} = append_entries_results) do
      empty_append_entries_counts =
        if append_entries_results.success do
          Map.update!(state.empty_append_entries_counts, append_entries_results.from, fn {num, :received} -> {num + 1, :confirmed} end)
        else
          Map.put(state.empty_append_entries_counts, append_entries_results.from, {0, :confirmed})
        end

      %__MODULE__{state | empty_append_entries_counts: empty_append_entries_counts}
    end

    # if three rounds of empty AppendEntries messages take place with the same leader, we consider the group stable
    def group_stable?(%State{} = state) do
      [{3, :confirmed}] ==
        state.empty_append_entries_counts
        |> Map.values()
        |> Enum.uniq()
    end
  end

  def start_link(states) do
    GenServer.start_link(__MODULE__, states)
  end

  def cast(nexus, to, message) do
    GenServer.cast(nexus, {:cast, to, node(), message})
  end

  def wait_until(nexus, condition) do
    GenServer.call(nexus, {:wait_until, condition}, 10_000)
  end

  def init(states) do
    name =
      :crypto.strong_rand_bytes(3)
      |> Base.encode16()

    nodes = Keyword.keys(states)

    states =
      Enum.map(states, fn {node, state} ->
        {node, %{state | name: name, other_nodes: List.delete(nodes, node), tracer_pid: self()}}
      end)

    for node <- nodes do
      :pong = Node.ping(node)
      {:module, Craft} = :rpc.call(node, Code, :ensure_loaded, [Craft])
    end

    Task.async_stream(states, fn {node, state} ->
      :rpc.call(node, Craft.Application, :start_member, [state])
    end)
    |> Stream.run()

    Task.async_stream(nodes, fn node ->
      :gen_statem.cast({Consensus.name(name), node}, :run)
    end)
    |> Stream.run()

    {:ok, %State{members: nodes}}
  end

  def handle_call({:wait_until, condition}, from, state) do
    {:noreply, %State{state | wait_until: {from, condition}}}
  end

  def handle_cast({:cast, {_, member} = to, _leader, %AppendEntries{} = append_entries}, %State{wait_until: {_, :group_stable}} = state) do
    :gen_statem.cast(to, append_entries)

    state = State.append_entries_received(state, member, append_entries)

    {:noreply, state}
  end

  def handle_cast({:cast, {_, leader} = to, _member, %AppendEntries.Results{} = append_entries_results}, %State{wait_until: {watcher, :group_stable}} = state) do
    :gen_statem.cast(to, append_entries_results)

    state = State.append_entries_results_received(state, leader, append_entries_results)

    if State.group_stable?(state) do
      GenServer.reply(watcher, state)

      {:noreply, %State{state | wait_until: nil}}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:cast, to, _from_node, message}, state) do
    :gen_statem.cast(to, message)

    {:noreply, state}
  end

  def handle_info({:trace, _time, from, :leader, :enter, :candidate, %CandidateState{current_term: current_term}}, state) do
    state = State.leader_elected(state, from, current_term)

    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # # @spec watch(member_nodes, until :: {:millisecs, pos_integer()}) | :group_stable
  # def watch(members, until \\ :group_stable)
  # def watch(members, millisecs: millisecs) do
  #   Process.send_after(self(), :stop_watching, millisecs)

  #   members |> State.new(millisecs) |> do_watch()
  # end
  # def watch(members, until), do: members |> State.new(until) |> do_watch()

  # defp do_watch(state) do
  #   receive do
  #     :stop_watching ->
  #       invocations = Enum.sort_by(state.genstatem_invocations, fn {time, _} -> time end)
  #       {:ok, %State{state | genstatem_invocations: invocations}}

  #     {:cast, _time, from, to, msg} ->
  #       case handle_cast(from, to, msg, state) do
  #         %State{} = state ->
  #           :gen_statem.cast(to, msg)

  #           do_watch(state)

  #         result ->
  #           result
  #       end

  #     {:trace, _time, from, :leader, :enter, :candidate, %CandidateState{current_term: current_term}} ->
  #       state
  #       # |> record_invocation(invocation)
  #       |> State.leader_elected(from, current_term)
  #       |> do_watch()

  #     {:trace, _, _, _, _} ->
  #       state
  #       # |> record_invocation(invocation)
  #       |> do_watch()
  #   end
  # end

  # defp handle_cast(_from, {_, to_node}, %AppendEntries{} = append_entries, %State{until: :group_stable} = state) do
  #   State.append_entries_received(state, to_node, append_entries)
  # end

  # defp handle_cast(_from, {_, to_node}, %AppendEntries.Results{} = append_entries_results, %State{until: :group_stable} = state) do
  #   state = State.append_entries_results_received(state, to_node, append_entries_results)

  #   if State.group_stable?(state) do
  #     {:ok, :group_stable, state}
  #   else
  #     state
  #   end
  # end

  # defp handle_cast(_, _, _, state), do: state

  # # defp record_invocation(state, invocation) do
  # #   invocation = {DateTime.utc_now(), invocation}
  # #   %State{state | genstatem_invocations: [invocation | state.genstatem_invocations]}
  # # end
end