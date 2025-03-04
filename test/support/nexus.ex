defmodule Craft.Nexus do
  @moduledoc """
  this module acts as a central hub for cluster messages and member traces
  """
  use GenServer

  alias Craft.Consensus.State, as: ConsensusState
  alias Craft.RPC.AppendEntries

  defmodule State do
    defstruct [
      members: [],
      term: -1,
      leader: nil,
      # num of consecutive successfully received and responded-to empty AppendEntries msgs
      empty_append_entries_counts: %{},
      genstatem_invocations: [],
      # watcher termination condition (:all_stable, :millisecs, etc)
      wait_until: {_watcher_from = nil, _condition = nil},
      # action = :drop | {:delay, msecs} | :forward | {:forward, modified_message}
      # fn message, nemesis_state -> {action, nemesis_state} end
      nemesis: nil,
      nemesis_state: nil
    ]

    def leader_elected(%State{term: term} = state, leader, new_term) when new_term > term do
      empty_append_entries_counts =
        state.members
        |> List.delete(leader)
        |> Enum.into(%{}, & {&1, {0, :confirmed}})

      %__MODULE__{state | empty_append_entries_counts: empty_append_entries_counts, leader: leader, term: new_term}
    end

    def append_entries_received(%State{term: term, leader: leader} = state, member, %AppendEntries{term: term, leader_id: leader} = append_entries) do
      empty_append_entries_counts = Map.update(state.empty_append_entries_counts, member, {0, append_entries}, fn {num, _} -> {num, append_entries} end)

      %__MODULE__{state | empty_append_entries_counts: empty_append_entries_counts}
    end

    def append_entries_results_received(%State{term: term, leader: leader} = state, leader, %AppendEntries.Results{term: term} = append_entries_results) do
      empty_append_entries_counts =
        with true <- append_entries_results.success,
             {num, %AppendEntries{entries: []}} <- Map.get(state.empty_append_entries_counts, append_entries_results.from) do
          Map.put(state.empty_append_entries_counts, append_entries_results.from, {num + 1, nil})
        else
          _ ->
            Map.put(state.empty_append_entries_counts, append_entries_results.from, {0, nil})
        end

      %__MODULE__{state | empty_append_entries_counts: empty_append_entries_counts}
    end

    # if three rounds of empty AppendEntries messages take place with the same leader, we consider the group stable
    def all_stable?(%State{} = state) do
      Enum.all?(state.empty_append_entries_counts, fn {_, {num, _}} -> num >= 3 end)
    end

    def majority_stable?(%State{} = state) do
      majority = div(Enum.count(state.empty_append_entries_counts), 2) + 1

      # + 1 since leader is automatically "stable"
      num_stable = Enum.count(state.empty_append_entries_counts, fn {_, {num, _}} -> num >= 3 end) + 1

      num_stable >= majority
    end
  end


  def start_link(members) do
    GenServer.start_link(__MODULE__, members)
  end

  defdelegate stop(pid), to: GenServer

  def cast(nexus, to, message) do
    GenServer.cast(nexus, {:cast, to, node(), message})
  end

  def wait_until(nexus, condition) do
    GenServer.call(nexus, {:wait_until, condition}, 10_000)
  end

  def set_nemesis(nexus, fun) when is_function(fun, 2) do
    GenServer.call(nexus, {:set_nemesis, fun})
  end


  def init(members) do
    {:ok, %State{members: members, nemesis: fn _, state -> {:forward, state} end}}
  end

  def handle_call({:wait_until, condition}, from, state) do
    state =
      if condition in [:all_stable, :majority_stable] do
        %State{state | empty_append_entries_counts: %{}}
      else
        state
      end

    {:noreply, %State{state | wait_until: {from, condition}}}
  end

  def handle_call({:set_nemesis, fun}, _from, state) do
    {:reply, :ok, %State{state | nemesis: fun}}
  end

  def handle_cast({:cast, to, from, message}, state) do
    {_, to_node} = to
    event = {:cast, to_node, from, message}
    {action, nemesis_state} = state.nemesis.(event, state.nemesis_state)

    case action do
      :drop ->
        :noop

      :forward ->
        :gen_statem.cast(to, message)

      {:forward, modified_message} ->
        :gen_statem.cast(to, modified_message)

      {:delay, msecs} ->
        :timer.apply_after(msecs, :gen_statem, :cast, [to, message])
    end

    state =
      case message do
        %AppendEntries{} = append_entries ->
          {_, member} = to

          State.append_entries_received(state, member, append_entries)

        %AppendEntries.Results{} = append_entries_results ->
          {_, leader} = to
          state = State.append_entries_results_received(state, leader, append_entries_results)

          case state.wait_until do
            {watcher, :all_stable} ->
              if State.all_stable?(state) do
                GenServer.reply(watcher, state)

                %State{state | wait_until: nil}
              else
                state
              end

            {watcher, :majority_stable} ->
              if State.majority_stable?(state) do
                GenServer.reply(watcher, state)

                %State{state | wait_until: nil}
              else
                state
              end

            _ ->
              state
          end

        _ ->
          state
      end

    {:noreply,  %State{state | nemesis_state: nemesis_state}}
  end

  def handle_info({:trace, _time, from, :leader, :enter, :candidate, %ConsensusState{current_term: current_term}}, state) do
    state = State.leader_elected(state, from, current_term)

    case state.wait_until do
      {watcher, :leader_elected} ->
        GenServer.reply(watcher, state)

      _ ->
        :noop
    end

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
