# if three rounds of empty AppendEntries messages take place with the same leader, we consider the group stable
defmodule Craft.Nexus.Stability do
  alias Craft.RPC.AppendEntries

  defmodule State do
    defstruct [:members, :leader, :counts, :proportion]
  end

  def init(nexus_state, proportion) do
    %State{
      members: nexus_state.members,
      leader: nexus_state.leader,
      proportion: proportion
    }
    |> reset_counts()
  end

  def handle_event(%{meta: %{trace: {:sent_msg, follower, %AppendEntries{} = append_entries}, node: leader}}, %State{leader: leader} = state) do
    counts = Map.update(state.counts, follower, {0, append_entries}, fn {num, _} -> {num, append_entries} end)

    wait_action(%{state | counts: counts})
  end

  # seeing a different leader causes counts to reset
  def handle_event(%{meta: %{trace: {:sent_msg, _follower, %AppendEntries{}}, node: leader}}, state) do
    state = %{state | leader: leader} |> reset_counts()

    {:cont, state}
  end

  def handle_event(%{meta: %{trace: {:sent_msg, leader, %AppendEntries.Results{} = append_entries_results}, node: follower}}, %State{leader: leader} = state) do
    counts =
      with true <- append_entries_results.success,
           {num, %AppendEntries{entries: []}} <- Map.get(state.counts, follower) do
        Map.put(state.counts, follower, {num + 1, nil})
      else
        _ ->
          # new member or AppendEntries wasn't empty
          Map.put(state.counts, follower, {0, nil})
      end

    wait_action(%{state | counts: counts})
  end

  def handle_event(_event, state), do: {:cont, state}

  defp reset_counts(state) do
    counts =
      state.members
      |> List.delete(state.leader)
      |> Enum.into(%{}, & {&1, {0, nil}})

    %{state | counts: counts}
  end

  defp wait_action(%State{proportion: :all} = state) do
    if Enum.all?(state.counts, fn {_, {num, _}} -> num >= 3 end) do
      :halt
    else
      {:cont, state}
    end
  end

  defp wait_action(%State{proportion: :majority} = state) do
    majority = div(Enum.count(state.counts), 2) + 1

    # + 1 since leader is automatically "stable"
    num_stable = Enum.count(state.counts, fn {_, {num, _}} -> num >= 3 end) + 1

    if num_stable >= majority do
      :halt
    else
      {:cont, state}
    end
  end
end
