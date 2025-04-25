defmodule Craft.Consensus.State.LeaderState do
  alias Craft.Consensus.State
  alias Craft.Consensus.State.Members
  alias Craft.Log.MembershipEntry
  alias Craft.Persistence
  alias Craft.RPC.AppendEntries
  alias Craft.RPC.InstallSnapshot
  alias Craft.SnapshotServer.SnapshotTransfer

  defstruct [
    :next_indices,
    :match_indices,
    :membership_change,
    :leadership_transfer,
    :last_quorum_at, # the last time we knew we were leader
    last_heartbeat_replies_at: %{}, # for CheckQuorum, voting members only
    snapshot_transfers: %{}
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

  defmodule SnapshotTransfer do
    # send entire snapshot as a single message to start, later we'll use :file.sendfile
    defstruct [:files]

    def new(state) do
      :FIXME
      # log_index = State.latest_snapshot_index(state)
      # {:ok, log_entry} = Persistence.fetch(state.persistence, log_index)

      # dir = Map.fetch!(state.snapshots, log_index)
      # files =
      #   dir
      #   |> File.ls!()
      #   |> Map.new(fn name ->
      #     file = dir |> Path.join(name) |> File.read!()
      #     {name, file}
      #   end)

      # %__MODULE__{
      #   log_index: log_index,
      #   log_entry: log_entry,
      #   files: files
      # }
    end

    #TODO: handle directories
    def receive(%__MODULE__{} = snapshot_transfer, data_dir) do
      File.mkdir_p!(data_dir)

      for {name, content} <- snapshot_transfer.files do
        data_dir
        |> Path.join(name)
        |> IO.inspect(label: "WRITING")
        |> File.write!(content)
      end
    end
  end
  # defmodule SnapshotTransfer do
  #   defstruct [
  #     :log_index,
  #     :dir,
  #     :streams
  #     :current_transfer # {file, handle, offset, chunk}
  #   ]

  #   defmodule FileTransfer do
  #     # TODO: make configurable (or adaptive based on network congestion)
  #     @snapshot_chunk_size 5 # bytes

  #     defstruct [:path, :handle, :chunk, offset: 0]

  #     def new(path) do
  #       {:ok, handle} = File.open(path, [:read, read_ahead: @snapshot_chunk_size])

  #       %__MODULE__{
  #         path: path,
  #         handle: handle,
  #         chunk: read(handle)
  #       }
  #     end

  #     def advance(%__MODULE__{} = file_transfer) do
  #       chunk = read(file_transfer.handle)

  #       %__MODULE__{
  #         file_transfer |
  #         chunk: chunk,
  #         offset: file_transfer.offset + byte_size(chunk)
  #       }
  #     end

  #     defp read(handle) do
  #       IO.read(handle, @snapshot_chunk_size)
  #     end
  #   end

  #   def new(state) do
  #     log_index = State.latest_snapshot_index(state)
  #     dir = Map.fetch!(state.snapshots, log_index)
  #     files = File.ls!(dir)
  #     next_file = List.first(files)
  #     file_transfer =
  #       dir
  #       |> Path.join(next_file)
  #       |> FileTransfer.new()

  #     %__MODULE__{
  #       log_index: log_index,
  #       dir: dir,
  #       files: {[], files},
  #       current_transfer: file_transfer
  #     }
  #   end
  # end

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
      last_quorum_at: :erlang.monotonic_time(:millisecond)
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
    # and also avoids work when no follower log appends took place (i.e. a heartbeat that doesnt append anything)
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
          num_members_with_index = Enum.count(match_indices_for_commitment, fn {_node, match_index} -> match_index >= index end)

          num_members_with_index >= State.quorum_needed(state)
        end)

      # only bump commit index when the quorum entry is from the current term (section 5.4.2)
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
    # we don't know where we match the followers log
    match_indices = Map.put(state.leader_state.match_indices, results.from, 0)
    state = %State{state | leader_state: %__MODULE__{state.leader_state | match_indices: match_indices}}

    # is the follower going to need a snapshot?
    if needs_snapshot?(state, results.from) do
      if state.machine.__craft_mutable__() do
        {:needs_snapshot, create_snapshot_transfer(state, results.from)}
      else
        {:needs_snapshot, state}
      end
    else
      next_indices = Map.update!(state.leader_state.next_indices, results.from, fn next_index -> next_index - 1 end)

      %State{state | leader_state: %__MODULE__{state.leader_state | next_indices: next_indices}}
    end
  end

  def create_snapshot_transfer(%State{} = state, node) do
    snapshot_transfers = Map.put(state.leader_state.snapshot_transfers, node, SnapshotTransfer.new(state))

    %State{state | leader_state: %__MODULE__{state.leader_state | snapshot_transfers: snapshot_transfers}}
  end

  def handle_install_snapshot_results(%State{} = state, %InstallSnapshot.Results{success: true} = results) do
    snapshot_transfers = Map.delete(state.leader_state.snapshot_transfers, results.from)

    leader_state =
      %__MODULE__{
        state.leader_state |
        snapshot_transfers: snapshot_transfers,
        match_indices: Map.put(state.leader_state.next_indices, results.from, results.latest_index),
        next_indices: Map.put(state.leader_state.next_indices, results.from, results.latest_index + 1)
      }

    %State{state | leader_state: leader_state}
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

  def bump_last_heartbeat_reply_at(%State{} = state, %AppendEntries.Results{} = results) do
    if Members.can_vote?(state.members, results.from) do
      last_heartbeat_replies_at = Map.put(state.leader_state.last_heartbeat_replies_at, results.from, {results.append_entries_sent_at, :erlang.monotonic_time(:millisecond)})
      state = %State{state | leader_state: %__MODULE__{state.leader_state | last_heartbeat_replies_at: last_heartbeat_replies_at}}

      # -1 since we're the leader
      num_replies_needed = State.quorum_needed(state) - 1

      quorum_making_sent_times =
        last_heartbeat_replies_at
        |> Enum.map(fn {_member, {sent_at, _received_at}} -> sent_at end)
        |> Enum.sort()
        |> Enum.slice(0, num_replies_needed)

      # if quorum was achieved, the most we can say is that we we were leader when the earliest AppendEntries was sent
      if Enum.count(quorum_making_sent_times) >= num_replies_needed do
        %State{state | leader_state: %__MODULE__{state.leader_state | last_quorum_at: List.first(quorum_making_sent_times)}}
      else
        state
      end
    else
      state
    end
  end
end
