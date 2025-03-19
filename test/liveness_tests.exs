defmodule Craft.LivenessTests do
  use Craft.NexusCase

  alias Craft.Nexus.Stability
  alias Craft.RPC.RequestVote
  alias Craft.SimpleMachine

  nexus_test "processes commands with a minimal quorum operational", %{nodes: nodes, name: name, nexus: nexus} do
    wait_until(nexus, {Stability, :all})

    {majority, minority} = Enum.split(nodes, div(Enum.count(nodes), 2) + 1)
    leader = List.first(majority)

    :ok = Craft.transfer_leadership(name, leader, majority)

    assert %{leader: ^leader} = wait_until(nexus, {Stability, :majority})

    nemesis(nexus, fn {:cast, to, from, _msg}, state ->
      if from in minority or to in minority do
        {:drop, state}
      else
        {:forward, state}
      end
    end)

    assert :ok = SimpleMachine.put(name, majority, :a, 123)
    assert {:ok, 123} = SimpleMachine.get(name, majority, :a)
    assert false
  end

  nexus_test "leader without majority connectivity will step down (CheckQuorum)", %{nodes: nodes, nexus: nexus} do
    %{leader: leader} = wait_until(nexus, {Stability, :all})

    majority =
      Enum.take(
        nodes -- [leader],
        div(Enum.count(nodes), 2) + 1
      )

    nemesis(nexus, fn {:cast, to, from, _msg}, state ->
      if from == leader and to in majority or from in majority and to == leader do
        {:drop, state}
      else
        {:forward, state}
      end
    end)

    %{leader: new_leader} = wait_until(nexus, {Stability, :majority})

    assert new_leader != leader
  end

  nexus_test "nodes isolated from the leader don't trigger needlessly disruptive and hopeless elections (PreVote)", %{nodes: nodes, nexus: nexus} do
    %{leader: leader, term: term} = wait_until(nexus, {Stability, :all})

    node_isolated_from_leader = Enum.random(nodes -- [leader])

    # isolate a node and then continue once it attempts and fails a pre-vote round
    nemesis_and_wait_until(
      nexus,
      fn {:cast, to, from, _msg}, state ->
        if from == leader and to == node_isolated_from_leader or from == node_isolated_from_leader and to == leader do
          {:drop, state}
        else
          {:forward, state}
        end
      end,
      fn
        {:cast, ^node_isolated_from_leader, from, %RequestVote.Results{pre_vote: true, vote_granted: false}}, remaining_prevote_denials ->
          remaining_prevote_denials = remaining_prevote_denials || MapSet.new(nodes -- [leader, node_isolated_from_leader])
          remaining_prevote_denials = MapSet.delete(remaining_prevote_denials, from)

          if Enum.empty?(remaining_prevote_denials) do
            :halt
          else
            {:cont, remaining_prevote_denials}
          end

        {:cast, ^node_isolated_from_leader, _to, %RequestVote{pre_vote: false}}, _state ->
          flunk "isolated node attempted leadership election"

          :halt

        _, state ->
          {:cont, state}
      end
    )

    %{leader: ^leader, term: ^term} = wait_until(nexus, {Stability, :majority})
  end
end

