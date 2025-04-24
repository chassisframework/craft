defmodule CraftTest do
  use Craft.NexusCase

  alias Craft.Nexus.Stability
  alias Craft.SimpleMachine

  nexus_test "starts a group, elects a leader, replicates logs, processes commands", %{name: name, nexus: nexus} do
    wait_until(nexus, {Stability, :all})

    assert :ok = SimpleMachine.put(name, :a, 123)
    assert {:ok, 123} = SimpleMachine.get(name, :a)
  end

  nexus_test "leadership transfer", %{nodes: nodes, name: name, nexus: nexus} do
    %{leader: leader} = wait_until(nexus, {Stability, :all})

    new_leader = Enum.random(nodes -- [leader])
    Craft.transfer_leadership(name, new_leader)

    assert %{leader: ^new_leader} = wait_until(nexus, {Stability, :all})
  end

  nexus_test "remove a member", %{nodes: nodes, name: name, nexus: nexus} do
    %{leader: leader} = wait_until(nexus, {Stability, :all})

    member = Enum.random(nodes -- [leader])
    Craft.remove_member(name, member)
    members = Craft.state(name) |> Map.keys()
    assert members == nodes -- [member]
  end

  nexus_test "add a member", %{nodes: nodes, name: name, nexus: nexus} do
    wait_until(nexus, {Stability, :all})

    [new_node] = Craft.TestCluster.spawn_nodes(1)
    Craft.add_member(name, new_node)

    wait_until(nexus, {Stability, :all})

    members = name |> Craft.state() |> Map.keys() |> MapSet.new()
    assert members == MapSet.new([new_node | nodes])

    # already started node
    Craft.remove_member(name, new_node)
    Craft.add_member(name, new_node)

    wait_until(nexus, {Stability, :all})

    members = name |> Craft.state() |> Map.keys() |> MapSet.new()
    assert members == MapSet.new([new_node | nodes])
  end
end
