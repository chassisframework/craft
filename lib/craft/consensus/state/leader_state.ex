defmodule Craft.Consensus.State.LeaderState do
  alias Craft.Consensus.State
  alias Craft.Consensus.State.LeaderState.QuorumStatus
  alias Craft.Consensus.State.Members
  alias Craft.Log.MembershipEntry
  alias Craft.Machine
  alias Craft.MemberCache
  alias Craft.Persistence
  alias Craft.RPC.AppendEntries
  alias Craft.RPC.InstallSnapshot
  alias Craft.SnapshotServer.SnapshotTransfer

  defstruct [
    :next_indices,
    :match_indices,
    :membership_change,
    :leadership_transfer,
    :quorum_status,
    :waiting_for_lease,
    snapshot_transfers: %{}
  ]

  defmodule MembershipChange do
    # action: :add | :remove
    defstruct [:action, :node, :log_index]
  end


  defmodule LeadershipTransfer do
    defstruct [
      :from, # {pid, ref}, from Consensus.cast, for transmission to the new leader via AppendEntries
      :current_candidate,
      candidates: MapSet.new()
    ]

    def new(transfer_to, from) do
      %__MODULE__{current_candidate: transfer_to, from: from}
    end

    def new(%State{} = state) do
      %__MODULE__{
        candidates: state.members.voting_nodes,
        from: :internal
      }
      |> next_transfer_candidate()
    end

    def next_transfer_candidate(%__MODULE__{} = leadership_transfer) do
      if Enum.empty?(leadership_transfer.candidates) do
        :error
      else
        candidate = Enum.random(leadership_transfer.candidates)
        candidates = MapSet.delete(leadership_transfer.candidates, candidate)

        %{leadership_transfer | current_candidate: candidate, candidates: candidates}
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

    %__MODULE__{
      next_indices: next_indices,
      match_indices: match_indices,
      quorum_status: QuorumStatus.new()
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

  def add_node(%State{} = state, node, log_index) do
    next_index = Persistence.latest_index(state.persistence) + 1
    next_indices = Map.put(state.leader_state.next_indices, node, next_index)
    match_indices = Map.put(state.leader_state.match_indices, node, 0)

    membership_change = %MembershipChange{action: :add, node: node, log_index: log_index}

    leader_state =
      %{
        state.leader_state |
        next_indices: next_indices,
        match_indices: match_indices,
        membership_change: membership_change
      }

    %{state | members: Members.add_member(state.members, node), leader_state: leader_state}
  end

  def remove_node(%State{} = state, node, log_index) do
    next_indices = Map.delete(state.leader_state.next_indices, node)
    match_indices = Map.delete(state.leader_state.match_indices, node)

    membership_change = %MembershipChange{action: :remove, node: node, log_index: log_index}

    snapshot_transfers = Map.delete(state.leader_state.snapshot_transfers, node)

    leader_state =
      %{
        state.leader_state |
        next_indices: next_indices,
        match_indices: match_indices,
        membership_change: membership_change,
        snapshot_transfers: snapshot_transfers
      }

    %{state | members: Members.remove_member(state.members, node), leader_state: leader_state}
  end

  def handle_append_entries_results(%State{} = state, %AppendEntries.Results{} = results) do
    # need to implement flow control in response to how laggy (and how laggy over time) a follower is
    # e.g. don't send larger messages until previous smaller messages can be acked
    {handle_results?, notify_machine?, _follower_lagging?, state} =
      if Members.can_vote?(state.members, results.from) do
        QuorumStatus.heartbeat_response_received(state, results)
      else
        # TODO: support non-voting followers properly
        {true, false, false, state}
      end

    state =
      if handle_results? do
        do_handle_append_entries_results(state, results)
      else
        state
      end

    MemberCache.update(state)

    if notify_machine? do
      # snapshotting truncates the log, so we want to make sure that all followers are caught up first
      # we don't want to delete a snapshot that's being downloaded, nor truncate the log before a follower
      # that's just pulled a snapshot can catch up
      # TODO: make log length configurable
      all_followers_caught_up = Enum.empty?(state.members.catching_up_nodes)
      log_too_long = Persistence.length(state.persistence) > 10_000
      # log_too_big = Persistence.log_size() > 100mb or 100 entries, etc

      Machine.quorum_reached(state, all_followers_caught_up && log_too_long)
    end

    state
  end

  defp do_handle_append_entries_results(%State{} = state, %AppendEntries.Results{success: true} = results) do
    # avoid work when no follower log appends took place (i.e. a heartbeat that doesnt append anything)
    if results.latest_index > state.leader_state.match_indices[results.from] do
      state = put_in(state.leader_state.match_indices[results.from], results.latest_index)
      state = put_in(state.leader_state.next_indices[results.from], results.latest_index + 1)
      # find the highest uncommitted match index shared by a majority of servers
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
          num_members_with_index = Enum.count(match_indices_for_commitment, fn {_node, match_index} -> match_index >= index end)

          num_members_with_index >= State.quorum_needed(state)
        end)

      # only bump commit index when the quorum entry is from the current term (section 5.4.2)
      with false <- is_nil(highest_uncommitted_match_index),
           {:ok, entry} <- Persistence.fetch(state.persistence, highest_uncommitted_match_index),
           true <- entry.term == state.current_term do
        %{state | commit_index: highest_uncommitted_match_index}
      else
        _ ->
          state
      end
    else
      state
    end
  end

  defp do_handle_append_entries_results(%State{} = state, %AppendEntries.Results{success: false} = results) do
    # we don't know where we match the follower's log
    state = put_in(state.leader_state.match_indices[results.from], 0)

    if needs_snapshot?(state, results.from) do
      if state.machine.__craft_mutable__() do
        {:needs_snapshot, create_snapshot_transfer(state, results.from)}
      else
        {:needs_snapshot, state}
      end
    else
      next_indices = Map.update!(state.leader_state.next_indices, results.from, fn next_index -> next_index - 1 end)

      put_in(state.leader_state.next_indices, next_indices)
    end
  end

  def create_snapshot_transfer(%State{leader_state: %__MODULE__{snapshot_transfers: snapshot_transfers}} = state, for_node) when is_map_key(snapshot_transfers, for_node), do: state

  def create_snapshot_transfer(%State{} = state, for_node) do
    {index, {remote_path, files}} = state.snapshot
    snapshot_transfer = SnapshotTransfer.new(remote_path, files)

    snapshot_transfers = Map.put(state.leader_state.snapshot_transfers, for_node, {index, snapshot_transfer})

    put_in(state.leader_state.snapshot_transfers, snapshot_transfers)
  end

  def handle_install_snapshot_results(%State{} = state, %InstallSnapshot.Results{success: true} = results) do
    snapshot_transfers = Map.delete(state.leader_state.snapshot_transfers, results.from)

    leader_state =
      %{
        state.leader_state |
        snapshot_transfers: snapshot_transfers,
        match_indices: Map.put(state.leader_state.match_indices, results.from, results.latest_index),
        next_indices: Map.put(state.leader_state.next_indices, results.from, results.latest_index + 1)
      }

    %{state | leader_state: leader_state}
  end

  def needs_snapshot?(%State{} = state, node) do
    not match?({:ok, _}, Persistence.fetch(state.persistence, state.leader_state.next_indices[node] - 1))
  end

  def sending_snapshot?(%State{} = state, node) do
    !!state.leader_state.snapshot_transfers[node]
  end

  def transfer_leadership(%State{} = state) do
    put_in(state.leader_state.leadership_transfer, LeadershipTransfer.new(state))
  end

  def transfer_leadership(%State{} = state, to_member, from \\ nil) do
    put_in(state.leader_state.leadership_transfer, LeadershipTransfer.new(to_member, from))
  end
end
