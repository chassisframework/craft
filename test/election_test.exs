defmodule Craft.ElectionTest do
  use Craft.NexusCase,
      parameterize: (for leases <- [true, false], do: %{leader_leases: leases})

  alias Craft.Consensus.State
  alias Craft.SimpleMachine
  alias Craft.Log.EmptyEntry
  alias Craft.Nexus.Stability
  alias Craft.Persistence
  alias Craft.Persistence.MapPersistence
  alias Craft.TestGroup

  @tag :unmanaged
  nexus_test "pre-chosen candidate becomes leader", %{nodes: nodes} do
    candidate = Enum.random(nodes)

    {:ok, name, nexus} = TestGroup.start_group(nodes, manual_start: true)

    TestGroup.replace_consensus_state(name, candidate, %{state: :candidate})

    TestGroup.run(name, nodes)

    assert %{leader: ^candidate, term: 0} = wait_until(nexus, {Stability, :all})

    Craft.stop_group(name)
  end

  describe "5.4.1 election restriction" do
    @tag :unmanaged
    nexus_test "deny votes to out-of-date candidate, and correct its log", %{nodes: nodes} do
      {:ok, name, nexus} = TestGroup.start_group(nodes, manual_start: true)

      empty_persistence = State.new("abc", nodes, MapPersistence, SimpleMachine, nil, nil).persistence

      shared_log =
        empty_persistence
        |> Persistence.append(%EmptyEntry{term: 0})
        |> Persistence.append(%EmptyEntry{term: 1})

      majority_log =
        shared_log
        |> Persistence.append(%EmptyEntry{term: 4})
        |> Persistence.append(%EmptyEntry{term: 4})

      out_of_date_log =
        shared_log
        |> Persistence.append(%EmptyEntry{term: 2})
        |> Persistence.append(%EmptyEntry{term: 2})
        |> Persistence.append(%EmptyEntry{term: 3})
        |> Persistence.append(%EmptyEntry{term: 3})

      candidate = Enum.random(nodes)

      TestGroup.replace_consensus_state(name, candidate, %{state: :candidate, persistence: out_of_date_log})

      for node <- nodes -- [candidate] do
        TestGroup.replace_consensus_state(name, node, %{persistence: majority_log})
      end

      TestGroup.run(name, nodes)

      assert %{leader: leader} = wait_until(nexus, {Stability, :all})
      assert leader != candidate

      states = Craft.state(name)

      {_leader_state, {leader_log, _metadata}} = get_in(states, [leader, :consensus])
      {_caught_up_follower_state, {caught_up_follower_log, _metadata}} = get_in(states, [candidate, :consensus])
      assert caught_up_follower_log == leader_log

      Craft.stop_group(name)
    end

    @tag :unmanaged
    nexus_test "most up-to-date member in a split-brain is elected and corrects out-of-date logs", %{nodes: nodes} do
      {:ok, name, nexus} = TestGroup.start_group(nodes, manual_start: true)

      empty_persistence = State.new("abc", nodes, MapPersistence, SimpleMachine, nil, nil).persistence

      shared_log =
        empty_persistence
        |> Persistence.append(%EmptyEntry{term: 0})
        |> Persistence.append(%EmptyEntry{term: 1})

      out_of_date_log =
        shared_log
        |> Persistence.append(%EmptyEntry{term: 2})
        |> Persistence.append(%EmptyEntry{term: 2})
        |> Persistence.append(%EmptyEntry{term: 3})
        |> Persistence.append(%EmptyEntry{term: 3})

      up_to_date_log =
        shared_log
        |> Persistence.append(%EmptyEntry{term: 4})
        |> Persistence.append(%EmptyEntry{term: 4})


      {majority, _minority} = Enum.split(nodes, div(Enum.count(nodes), 2) + 1)
      candidate = Enum.random(majority)

      TestGroup.replace_consensus_state(name, candidate, %{state: :candidate, persistence: up_to_date_log})

      for node <- majority -- [candidate] do
        TestGroup.replace_consensus_state(name, node, %{persistence: out_of_date_log})
      end

      nemesis(nexus, fn {:cast, to, from, _msg} ->
        if from in majority and to in majority do
          :forward
        else
          :drop
        end
      end)

      TestGroup.run(name, nodes)

      assert %{leader: ^candidate} = wait_until(nexus, {Stability, :majority})
      states = Map.new(majority, &Craft.state(name, &1))

      {_leader_state, {leader_log, _metadata}} = get_in(states, [candidate, :consensus])
      for node <- majority -- [candidate] do
        {_follower_state, {follower_log, _metadata}} = get_in(states, [node, :consensus])

        assert follower_log == leader_log
      end

      Craft.stop_group(name)
    end
  end
end
