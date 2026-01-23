defmodule Craft.Consensus.State do
  alias Craft.Configuration
  alias Craft.Consensus.State.Election
  alias Craft.Consensus.State.LeaderState
  alias Craft.Consensus.State.Members
  alias Craft.Log.MembershipEntry
  alias Craft.Log.SnapshotEntry
  alias Craft.MemberCache
  alias Craft.Message.RequestVote
  alias Craft.Persistence
  alias Craft.Persistence.Metadata

  require Logger

  defstruct [
    {:state, :lonely},
    :name,
    :members,
    :persistence,
    :machine,
    :leader_id,
    :global_clock,
    :lease_expires_at,
    :snapshot,
    {:current_term, -1},
    {:commit_index, 0},

    :leader_state,
    :voted_for, # lonely and follower
    :election, # lonely and candidate
    :leadership_transfer_request_id, # candidate only
    :incoming_snapshot_transfer, # receiving_snapshot only

    :nexus_pid
  ]

  def new(name, nodes, persistence, machine, global_clock, nexus_pid \\ nil) do
    persistence = Persistence.new(name, persistence)

    # if we're restoring state from disk, search the log backwards for group members
    members =
      if nodes do
        Members.new(nodes)
      else
        entry =
          Persistence.reverse_find(persistence, fn
            %MembershipEntry{} -> true
            %SnapshotEntry{} -> true
            _ -> false
          end)

        # fall back to the nodes that the cluster was originally initialized with, the leader should contact us soon.
        if entry do
          entry.members
        else
          %{nodes: nodes} = Configuration.find(name)

          Members.new(nodes)
        end
      end

    %__MODULE__{
      name: name,
      members: members,
      persistence: persistence,
      machine: machine,
      global_clock: global_clock,
      nexus_pid: nexus_pid
    }
    |> Metadata.load()
  end

  def set_current_term(%__MODULE__{} = state, new_current_term, voted_for \\ nil) do
    state =
      %{
        state |
        current_term: new_current_term,
        voted_for: voted_for
      }

    Metadata.write(state)
  end

  def become_lonely(%__MODULE__{} = state, new_current_term) do
    state
    |> become_lonely()
    |> set_current_term(new_current_term)
  end

  def become_lonely(%__MODULE__{} = state) do
    %{
      state |
      state: :lonely,
      leader_state: nil,
      leader_id: nil,
      leadership_transfer_request_id: nil,
      election: Election.new(state.members)
    }
    |> release_buffer()
    |> update_cache()
  end

  def become_follower(%__MODULE__{} = state) do
    %{
      state |
      state: :follower,
      leader_state: nil,
      leadership_transfer_request_id: nil,
      election: nil
    }
    |> release_buffer()
    |> update_cache()
  end

  def become_receiving_snapshot(%__MODULE__{} = state) do
    %{
      state |
      state: :receiving_snapshot,
      leader_state: nil,
      leadership_transfer_request_id: nil,
      election: nil
    }
    |> release_buffer()
    |> update_cache()
  end

  # leadership_transfer_request_id set directly in Consensus
  # this makes test tracing easier as we don't need to special-case a tuple as `data` in Consensus.Tracer
  def become_candidate(%__MODULE__{} = state) do
    %{
      state |
      state: :candidate,
      leader_state: nil,
      election: Election.new(state.members),
    }
    |> set_current_term(state.current_term + 1, node())
    |> release_buffer()
    |> update_cache()
  end

  # the leader buffers log entry and writes them in batches
  def become_leader(%__MODULE__{} = state) do
    %{
      state |
      state: :leader,
      leader_id: node(),
      leader_state: LeaderState.new(state),
      election: nil
    }
    |> set_current_term(state.current_term)
    |> update_cache()
  end

  defp release_buffer(%__MODULE__{} = state) do
    %{state | persistence: Persistence.release_buffer(state.persistence)}
  end

  defp update_cache(%__MODULE__{state: :leader} = state) do
    MemberCache.leader_update(state)
    MemberCache.set_leader_ready(state.name, false)

    state
  end

  defp update_cache(%__MODULE__{} = state) do
    MemberCache.non_leader_update(state)
    MemberCache.set_leader_ready(state.name, nil)

    state
  end

  # voting for others

  def vote_for?(%__MODULE__{current_term: current_term}, %RequestVote{term: term}) when term < current_term, do: false

  def vote_for?(%__MODULE__{} = state, %RequestVote{} = request_vote) do
    request_vote.last_log_term > Persistence.latest_term(state.persistence) ||
    (request_vote.last_log_term == Persistence.latest_term(state.persistence) && request_vote.last_log_index >= Persistence.latest_index(state.persistence))
  end

  # holding pre-vote and leadership elections

  def record_vote(%__MODULE__{} = state, %RequestVote.Results{} = results) do
    %{state | election: Election.record_vote(state.election, results)}
  end

  def election_result(%__MODULE__{} = state) do
    Election.election_result(state.election, quorum_needed(state))
  end

  def quorum_needed(%__MODULE__{} = state) do
    num_members = MapSet.size(state.members.voting_nodes)

    div(num_members, 2) + 1
  end

  def snapshot_ready(%__MODULE__{} = state, index, path_or_content) do
    {:ok, %{term: term}} = Persistence.fetch(state.persistence, index)
    persistence = Persistence.truncate(state.persistence, index, SnapshotEntry.new(state, term, path_or_content))

    state =
      if state.leader_state do
        put_in(state.leader_state.snapshot_transfers, %{})
      else
        state
      end

    with true <- state.machine.__craft_mutable__(),
         {_index, {path, _}} <- state.snapshot do
      Configuration.data_dir()
      |> Path.join(path)
      |> File.rm_rf()
    end

    %{state | snapshot: {index, path_or_content}, persistence: persistence}
  end
end
