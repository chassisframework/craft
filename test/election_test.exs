defmodule Craft.ElectionTest do
  use Craft.NexusCase,
      parameterize: (for leases <- [true, false], do: %{leader_leases: leases})

  alias Craft.Log.EmptyEntry
  alias Craft.Nexus.Stability
  alias Craft.Persistence
  alias Craft.TestGroup

  describe "5.4.1 election restriction" do
    @tag :unmanaged
    nexus_test "deny votes to out-of-date candidate, and correct its log", %{nodes: nodes} do
      {:ok, name, nexus} = TestGroup.start_group(nodes, manual_start: true)
      candidate = Enum.random(nodes)

      TestGroup.replace_consensus_state(
        name,
        candidate,
        quote do
          fn state ->
            alias Craft.Persistence
            alias Craft.Log.EmptyEntry

            out_of_date_log =
              state.persistence
              |> Persistence.rewind(-1)
              |> Persistence.append(%EmptyEntry{term: 0})
              |> Persistence.append(%EmptyEntry{term: 1})
              |> Persistence.append(%EmptyEntry{term: 2})
              |> Persistence.append(%EmptyEntry{term: 2})
              |> Persistence.append(%EmptyEntry{term: 3})
              |> Persistence.append(%EmptyEntry{term: 3})

            %{state | state: :candidate, persistence: out_of_date_log}
          end
        end)

      for node <- nodes -- [candidate] do
        TestGroup.replace_consensus_state(
          name,
          node,
          quote do
            fn state ->
              alias Craft.Persistence
              alias Craft.Log.EmptyEntry

              majority_log =
                state.persistence
                |> Persistence.rewind(-1)
                |> Persistence.append(%EmptyEntry{term: 0})
                |> Persistence.append(%EmptyEntry{term: 1})
                |> Persistence.append(%EmptyEntry{term: 4})
                |> Persistence.append(%EmptyEntry{term: 4})

              %{state | persistence: majority_log}
            end
        end)
      end

      TestGroup.run(name, nodes)

      assert %{leader: leader} = wait_until(nexus, {Stability, :all})
      assert leader != candidate

      states = Craft.state(name)

      {_leader_state, leader_state} = get_in(states, [leader, :consensus])
      {_caught_up_follower_state, caught_up_follower_state} = get_in(states, [candidate, :consensus])
      assert caught_up_follower_state.log == leader_state.log

      Craft.stop_group(name)
    end

    @tag :unmanaged
    nexus_test "most up-to-date member in a split-brain is elected and corrects out-of-date logs", %{nodes: nodes} do
      {:ok, name, nexus} = TestGroup.start_group(nodes, manual_start: true)

      {majority, _minority} = Enum.split(nodes, div(Enum.count(nodes), 2) + 1)
      candidate = Enum.random(majority)

      TestGroup.replace_consensus_state(
        name,
        candidate,
        quote do
          fn state ->
            alias Craft.Persistence
            alias Craft.Log.EmptyEntry

            up_to_date_log =
              state.persistence
              |> Persistence.rewind(-1)
              |> Persistence.append(%EmptyEntry{term: 0})
              |> Persistence.append(%EmptyEntry{term: 1})
              |> Persistence.append(%EmptyEntry{term: 4})
              |> Persistence.append(%EmptyEntry{term: 4})

            %{state | state: :candidate, persistence: up_to_date_log}
          end
        end)

      for node <- nodes -- [candidate] do
        TestGroup.replace_consensus_state(
          name,
          node,
          quote do
            fn state ->
              alias Craft.Persistence
              alias Craft.Log.EmptyEntry

              out_of_date_log =
                state.persistence
                |> Persistence.rewind(-1)
                |> Persistence.append(%EmptyEntry{term: 0})
                |> Persistence.append(%EmptyEntry{term: 1})
                |> Persistence.append(%EmptyEntry{term: 2})
                |> Persistence.append(%EmptyEntry{term: 2})
                |> Persistence.append(%EmptyEntry{term: 3})
                |> Persistence.append(%EmptyEntry{term: 3})

              %{state | persistence: out_of_date_log}
            end
        end)
      end

      nemesis(nexus, fn {:sent_msg, to, from, _msg} ->
        if from in majority and to in majority do
          :forward
        else
          :drop
        end
      end)

      TestGroup.run(name, nodes)

      assert %{leader: ^candidate} = wait_until(nexus, {Stability, :majority})
      states = Map.new(majority, &Craft.state(name, &1))

      {_leader_state, leader_state} = get_in(states, [candidate, :consensus])
      for node <- majority -- [candidate] do
        {_follower_state, follower_state} = get_in(states, [node, :consensus])

        assert follower_state.log == leader_state.log
      end

      Craft.stop_group(name)
    end
  end
end
