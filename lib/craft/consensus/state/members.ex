defmodule Craft.Consensus.State.Members do
  defstruct [
    :catching_up_nodes,
    :non_voting_nodes,
    :voting_nodes
  ]

  def new(voting_nodes, non_voting_members \\ []) do
    %__MODULE__{
      catching_up_nodes: MapSet.new(),
      voting_nodes: MapSet.new(voting_nodes),
      non_voting_nodes: MapSet.new(non_voting_members)
    }
  end

  def add_member(%__MODULE__{} = members, node) do
    if MapSet.member?(members.voting_nodes, node) or MapSet.member?(members.non_voting_nodes, node) do
      raise "member already added"
    end

    %__MODULE__{
      members |
      catching_up_nodes: MapSet.put(members.catching_up_nodes, node)
    }
  end

  def remove_member(%__MODULE__{} = members, node) do
    %__MODULE__{
      members |
      catching_up_nodes: MapSet.delete(members.catching_up_nodes, node),
      voting_nodes: MapSet.delete(members.voting_nodes, node),
      non_voting_nodes: MapSet.delete(members.non_voting_nodes, node)
    }
  end

  def allow_node_to_vote(%__MODULE__{} = members, node) do
    %__MODULE__{
      members |
      voting_nodes: MapSet.put(members.voting_nodes, node),
      catching_up_nodes: MapSet.delete(members.catching_up_nodes, node),
      non_voting_nodes: MapSet.delete(members.non_voting_nodes, node)
    }
  end

  def can_vote?(%__MODULE__{} = members, member) do
    MapSet.member?(members.voting_nodes, member)
  end

  def this_node_can_vote?(%__MODULE__{} = members) do
    can_vote?(members, node())
  end

  def other_voting_nodes(%__MODULE__{} = members) do
    MapSet.delete(members.voting_nodes, node())
  end

  def other_nodes(%__MODULE__{} = members) do
    members.voting_nodes
    |> MapSet.union(members.catching_up_nodes)
    |> MapSet.union(members.non_voting_nodes)
    |> MapSet.delete(node())
  end

  def all_nodes(%__MODULE__{} = members) do
    members.voting_nodes
    |> MapSet.union(members.catching_up_nodes)
    |> MapSet.union(members.non_voting_nodes)
  end
end
