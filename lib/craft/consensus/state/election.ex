defmodule Craft.Consensus.State.Election do
  alias Craft.Consensus.State.Members
  alias Craft.Message.RequestVote

  defstruct [
    num_votes: 0,
    received_votes_from: MapSet.new(),
  ]

  def new(%Members{} = members) do
    # this node might not be voting in majorities if it is being removed from the cluster (section 4.2.2)
    if Members.this_node_can_vote?(members) do
      %__MODULE__{
        num_votes: 1,
        received_votes_from: MapSet.new([node()])
      }
    else
      %__MODULE__{}
    end
  end

  def record_vote(%__MODULE__{} = election, %RequestVote.Results{} = results) do
    if not MapSet.member?(election.received_votes_from, results.from) do
      %{
        election |
        num_votes: election.num_votes + if(results.vote_granted, do: 1, else: 0),
        received_votes_from: MapSet.put(election.received_votes_from, results.from)
      }
    else
      election
    end
  end

  def election_result(%__MODULE__{} = election, quorum_needed) do
    num_voted_no = MapSet.size(election.received_votes_from) - election.num_votes

    cond do
      election.num_votes >= quorum_needed ->
        :won

      num_voted_no >= quorum_needed ->
        :lost

      true ->
        :pending
    end
  end
end
