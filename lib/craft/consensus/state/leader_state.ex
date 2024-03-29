defmodule Craft.Consensus.State.LeaderState do
  alias Craft.Consensus
  alias Craft.Consensus.State
  alias Craft.Consensus.State.Members
  alias Craft.Persistence
  alias Craft.Log.MembershipEntry
  alias Craft.RPC.AppendEntries

  defstruct [
    :next_indices,
    :match_indices,
    :last_heartbeat_replies_at, # for CheckQuorum voting members only
    :membership_change,
    :leadership_transfer
  ]

  defmodule MembershipChange do
    # action: :add | :remove
    defstruct [:action, :node, :from, :log_index]
  end

  defmodule LeadershipTransfer do
    defstruct [
      :from, # {pid, ref}, from Consensus.cast, for transmission to the new leader via AppenEntries
      :current_candidate,
      candidates: MapSet.new()
    ]

    def new(transfer_to, from) do
      %__MODULE__{current_candidate: transfer_to, from: from}
    end

    def new(%State{} = state) do
      %__MODULE__{candidates: state.members.voting_nodes}
      |> next_transfer_candidate()
    end

    def next_transfer_candidate(%__MODULE__{} = leadership_transfer) do
      if Enum.empty?(leadership_transfer.candidates) do
        :error
      else
        candidate = Enum.random(leadership_transfer.candidates)
        candidates = MapSet.delete(leadership_transfer.candidates, candidate)

        %__MODULE__{leadership_transfer | current_candidate: candidate, candidates: candidates}
      end
    end
  end

  # FIXME: if config_change_in_progress, reconstruct :membership_change?
  # this may need to happen after new leader figures out the commit index
  # may need to have stored the :membership_change in the MembershipEntry
  def new(%State{} = state) do
    next_index = Persistence.latest_index(state.persistence) + 1
    next_indices = state.members |> Members.other_nodes() |> Map.new(&{&1, next_index})
    match_indices = state.members |> Members.other_nodes() |> Map.new(&{&1, 0})
    last_heartbeat_replies_at = state.members |> Members.other_voting_nodes() |> Map.new(&{&1, :erlang.monotonic_time(:millisecond)})

    %__MODULE__{
      next_indices: next_indices,
      match_indices: match_indices,
      last_heartbeat_replies_at: last_heartbeat_replies_at
    }
  end

  def config_change_in_progress?(%State{} = state) do
    state.persistence
    |> Persistence.fetch_from(state.commit_index + 1)
    |> Enum.any?(fn
      %MembershipEntry{} -> true
      _ -> false
    end)
  end

  def add_node(%State{} = state, node, from, log_index) do
    next_index = Persistence.latest_index(state.persistence) + 1
    next_indices = Map.put(state.leader_state.next_indices, node, next_index)
    match_indices = Map.put(state.leader_state.match_indices, node, 0)

    membership_change = %MembershipChange{action: :add, node: node, from: from, log_index: log_index}

    leader_state =
      %__MODULE__{
        state.leader_state |
        next_indices: next_indices,
        match_indices: match_indices,
        membership_change: membership_change
      }

    %State{state | members: Members.add_member(state.members, node), leader_state: leader_state}
  end

  def remove_node(%State{} = state, node, from, log_index) do
    next_indices = Map.delete(state.leader_state.next_indices, node)
    match_indices = Map.delete(state.leader_state.match_indices, node)
    last_heartbeat_replies_at = Map.delete(state.leader_state.last_heartbeat_replies_at, node)

    membership_change = %MembershipChange{action: :remove, node: node, from: from, log_index: log_index}

    leader_state =
      %__MODULE__{
        state.leader_state |
        next_indices: next_indices,
        match_indices: match_indices,
        membership_change: membership_change,
        last_heartbeat_replies_at: last_heartbeat_replies_at
      }

    %State{state | members: Members.remove_member(state.members, node), leader_state: leader_state}
  end

  def handle_append_entries_results(%State{} = state, %AppendEntries.Results{success: true} = results) do
    # accounts for the possibility of stale AppendEntries results (due to pathological network reordering)
    # and also avoids work when no follower log appends took place
    if results.latest_index > state.leader_state.match_indices[results.from] do
      match_indices = Map.put(state.leader_state.match_indices, results.from, results.latest_index)
      next_indices = Map.put(state.leader_state.next_indices, results.from, results.latest_index + 1)
      state = %State{state | leader_state: %__MODULE__{state.leader_state | next_indices: next_indices, match_indices: match_indices}}
      # find the highest uncommitted match index shared by a majority of servers
      # this can be optimized to some degree (mapset, gb_tree, etc...)
      # also optimized by pre-computing quorum requirement and storing in state
      #
      # when we become leader, match indexes work their way up from zero non-uniformly
      # so it's entirely possible that we don't find a quorum of followers with a match index
      # until match indexes work their way up to parity
      #
      # this node (the leader), might not be voting in majorities if it is removing itself
      # from the cluster (section 4.2.2)
      #
      match_indices_for_commitment =
        if Members.this_node_can_vote?(state.members) do
          Map.put(state.leader_state.match_indices, node(), Persistence.latest_index(state.persistence))
        else
          state.leader_state.match_indices
        end

      highest_uncommitted_match_index =
        match_indices_for_commitment
        |> Map.values()
        |> Enum.filter(fn index -> index >= state.commit_index end)
        |> Enum.uniq()
        |> Enum.sort()
        |> Enum.reverse()
        |> Enum.find(fn index ->
          num_members_with_index = Enum.count(state.leader_state.match_indices, fn {_node, match_index} -> match_index >= index end)
          Consensus.quorum_reached?(state, num_members_with_index)
        end)

      with false <- is_nil(highest_uncommitted_match_index),
           {:ok, entry} <- Persistence.fetch(state.persistence, highest_uncommitted_match_index),
           true <- entry.term == state.current_term do
        %State{state | commit_index: highest_uncommitted_match_index}
      else
        _ ->
          state
      end
    else
      state
    end
  end

  def handle_append_entries_results(%State{} = state, %AppendEntries.Results{success: false} = results) do
    next_indices = Map.update!(state.leader_state.next_indices, results.from, fn next_index -> next_index - 1 end)

    %State{state | leader_state: %__MODULE__{state.leader_state | next_indices: next_indices}}
  end

  def transfer_leadership(%State{} = state) do
    put_in(state.leader_state.leadership_transfer, LeadershipTransfer.new(state))
  end

  def transfer_leadership(%State{} = state, to_member, from \\ nil) do
    put_in(state.leader_state.leadership_transfer, LeadershipTransfer.new(to_member, from))
  end

  def bump_last_heartbeat_reply_at(%State{} = state, member) do
    if Members.can_vote?(state.members, member) do
      last_heartbeat_replies_at = Map.put(state.leader_state.last_heartbeat_replies_at, member, :erlang.monotonic_time(:millisecond))

      %State{state | leader_state: %__MODULE__{state.leader_state | last_heartbeat_replies_at: last_heartbeat_replies_at}}
    else
      state
    end
  end
end
