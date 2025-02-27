defmodule CraftTest do
  use ExUnit.Case
  alias Craft.Consensus.State
  alias Craft.Persistence.MapPersistence
  alias Craft.Nexus

  alias Craft.SimpleMachine
  alias Craft.TestCluster
  alias Craft.TestHelper

  import Nexus, only: [wait_until: 2]

  setup_all do
    [
      nodes: TestCluster.spawn_nodes(5)
    ]
  end

  # describe "smoke tests" do
  #   test "starts a group, elects a leader, replicates logs, processes commands" do

  #   end
  # end

  test "commands", %{nodes: nodes} do
    {:ok, name, nexus} = TestHelper.start_group(nodes)

    wait_until(nexus, :group_stable)

    assert :ok = SimpleMachine.put(name, nodes, :a, 123)
    assert {:ok, 123} = SimpleMachine.get(name, nodes, :a)

    Craft.stop_group(name, nodes)
    Nexus.stop(nexus)
  end

  # test api design:
  #
  # series of:
  #   {until, nemesis}
  #
  #  ex:
  #  start in split brain, wait for stability (leader elected, etc), send a command to the group, then heal connectivity and wait for stability
  #  [{:group_stable, SplitBrain},
  #   fn group -> :ok = Craft.command(group, :some_command) end,
  #   {:group_stable, nil}]
end
