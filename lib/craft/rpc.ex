defmodule Craft.RPC do
  @moduledoc false

  alias Craft.Consensus
  alias Craft.Consensus.FollowerState
  alias Craft.Consensus.CandidateState
  alias Craft.Consensus.LeaderState
  alias Craft.RPC.RequestVote
  alias Craft.RPC.AppendEntries

  # def start(name, to_node, {_m, _f, _a} = request) do
  #   ARQ.start(request, supervisor)
  # end

  def request_vote(%CandidateState{name: name, other_nodes: other_nodes} = state) do
    request_vote = RequestVote.new(state)

    for to_node <- other_nodes do
      :gen_statem.cast({Consensus.name(name), to_node}, request_vote)
    end
  end

  def respond_vote(%RequestVote{candidate_id: candidate_id} = request_vote, %state_name{name: name} = state) when state_name in [FollowerState, CandidateState] do
    :gen_statem.cast({Consensus.name(name), candidate_id}, RequestVote.Results.new(request_vote, state))
  end

  def append_entries(%LeaderState{name: name, other_nodes: other_nodes} = state) do
    for to_node <- other_nodes do
      append_entries = AppendEntries.new(state, to_node)

      :gen_statem.cast({Consensus.name(name), to_node}, append_entries)
    end
  end
end
