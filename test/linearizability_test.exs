defmodule Craft.LinearizabilityTest do
  use Craft.NexusCase,
      parameterize: (for leases <- [true, false], do: %{leader_leases: leases})

  alias Craft.Nexus.Stability
  alias Craft.ParallelClients
  alias Craft.Linearizability

  @moduletag timeout: :timer.minutes(20)

  nexus_test "under stable conditions", ctx do
    wait_until(ctx.nexus, {Stability, :all})

    num_clients = 1
    num_commands = 10

    ctx
    |> random_request_fun()
    |> ParallelClients.run(num_clients, num_commands)
    |> assert_linearizable()
  end

  nexus_test "during leadership transfer", %{nodes: nodes, name: name, nexus: nexus} = ctx do
    %{leader: leader} = wait_until(nexus, {Stability, :all})

    num_clients = 10

    clients =
      ctx
      |> random_request_fun()
      |> ParallelClients.start(num_clients)

    Process.sleep(300)

    new_leader = Enum.random(nodes -- [leader])
    :ok = Craft.transfer_leadership(name, new_leader)

    Process.sleep(300)

    history = ParallelClients.stop(clients)

    assert %{leader: ^new_leader} = wait_until(nexus, {Stability, :all})

    assert_linearizable(history)
    # assert {:ok, _linearized_history, _ignored_ops} = Craft.Linearizability.linearize(history, Craft.SimpleMachine)
    # File.write!("history", :erlang.term_to_binary({linearized_history, ignored_ops}))
    # Craft.Linearizability.Visualization.to_file(nil)
  end

  nexus_test "when leader loses majority connectivity", %{nodes: nodes, nexus: nexus} = ctx do
    %{leader: leader} = wait_until(nexus, {Stability, :all})

    majority =
      Enum.take(
        nodes -- [leader],
        div(Enum.count(nodes), 2) + 1
      )

    num_clients = 10

    clients =
      ctx
      |> random_request_fun()
      |> ParallelClients.start(num_clients)

    Process.sleep(300)

    nemesis(nexus, fn {:cast, to, from, _msg} ->
      if from == leader and to in majority or from in majority and to == leader do
        :drop
      else
        :forward
      end
    end)

    Process.sleep(300)

    history = ParallelClients.stop(clients)

    wait_until(nexus, {Stability, :majority})

    assert_linearizable(history)
  end

  # TODO: stop and start servers
  nexus_test "during utter chaos", %{nexus: nexus} = ctx do
    wait_until(nexus, {Stability, :all})

    num_clients = 10

    clients =
      ctx
      |> random_request_fun()
      |> ParallelClients.start(num_clients)

    Process.sleep(300)

    nemesis(nexus, fn _ ->
      if :rand.uniform(100) <= 50 do
        :drop
      else
        :forward
      end
    end)

    Process.sleep(3000)

    history = ParallelClients.stop(clients)

    assert_linearizable(history)
  end

  def assert_linearizable(history) do
    assert {:ok, _linearized_history, _ignored_ops} = Linearizability.linearize(history, Craft.SimpleMachine)
  end

  defp random_request_fun(ctx) do
    fn i ->
      value =
        self()
        |> :erlang.pid_to_list()
        |> :erlang.list_to_binary()
        |> String.trim("<")
        |> String.trim(">")

      if :rand.uniform(100) > 50 do
        command = {:put, :a, "#{value}_#{i}"}
        {{:write, command}, Craft.command(command, ctx.name)}
      else
        query = {:get, :a}
        {{:read, query}, Craft.query(query, ctx.name)}
      end
    end
  end
end
