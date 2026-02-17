defmodule Craft.LinearizabilityTest do
  use Craft.NexusCase,
      parameterize: (for leases <- [true, false], do: %{leader_leases: leases})

  alias Craft.Linearizability
  alias Craft.Nexus.Stability
  alias Craft.ParallelClients

  @moduletag timeout: :timer.minutes(20)

  nexus_test "under stable conditions", ctx do
    wait_until(ctx.nexus, {Stability, :all})

    num_clients = 10
    num_commands = 20

    ctx
    |> random_request_fun()
    |> ParallelClients.run(num_clients, num_commands, ctx.nexus)
    |> assert_linearizable()
  end

  nexus_test "during leadership transfer", %{nodes: nodes, name: name, nexus: nexus} = ctx do
    %{leader: leader} = wait_until(nexus, {Stability, :all})

    num_clients = 10

    clients =
      ctx
      |> random_request_fun()
      |> ParallelClients.start(num_clients, ctx.nexus)

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
      |> ParallelClients.start(num_clients, nexus)

    Process.sleep(300)

    nemesis(nexus, fn {:sent_msg, to, from, _msg} ->
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

  # TODO: stop and start servers, delay messages etc
  nexus_test "during utter chaos", %{nexus: nexus} = ctx do
    wait_until(nexus, {Stability, :all})

    nemesis(nexus, fn _ ->
      if :rand.uniform(100) <= 50 do
        :drop
      else
        :forward
      end
    end)

    num_clients = 10

    clients =
      ctx
      |> random_request_fun()
      |> ParallelClients.start(num_clients, nexus)

    Process.sleep(3000)

    history = ParallelClients.stop(clients)

    assert_linearizable(history)
  end

  nexus_test "write-optimized machine", %{name: name, nexus: nexus} = ctx do
    wait_until(nexus, {Stability, :all})

    num_clients = 10

    :ok = Craft.switch_mode(name, :write_optimized)

    clients =
      ctx
      |> random_request_fun()
      |> ParallelClients.start(num_clients, ctx.nexus)

    Process.sleep(600)

    history = ParallelClients.stop(clients)

    assert_linearizable(history)
  end

  nexus_test "switch to write-optimized machine", %{name: name, nexus: nexus} = ctx do
    wait_until(nexus, {Stability, :all})

    num_clients = 10

    clients =
      ctx
      |> random_request_fun()
      |> ParallelClients.start(num_clients, ctx.nexus)

    Process.sleep(300)

    :ok = Craft.switch_mode(name, :write_optimized)

    Process.sleep(300)

    history = ParallelClients.stop(clients)

    assert_linearizable(history)
  end

  def assert_linearizable(history) do
    case Linearizability.linearize(history, Craft.SimpleMachine) do
      {:ok, _linearized_history, _ignored_ops} ->
        :ok

      :error ->
        rand = :crypto.strong_rand_bytes(8) |> Base.encode16()
        filename = "#{__MODULE__}_history_#{rand}.erlterm"
        File.write(filename, :erlang.term_to_binary(history))

        flunk "linearizability violation detected, history written to #{filename}"
    end
  end

  defp random_request_fun(ctx) do
    fn i ->
      value =
        self()
        |> :erlang.pid_to_list()
        |> :erlang.list_to_binary()
        |> String.trim("<")
        |> String.trim(">")

      case :rand.uniform(100) do
        val when val > 50 ->
          command = {:put, :a, "#{value}_#{i}"}
          {{:write, command}, Craft.command(command, ctx.name)}

        val when val > 25  ->
          query = {:get, :a}
          {{:read, query}, Craft.query(query, ctx.name)}

        _val ->
          # {{:read, {:get_parallel, :a, send_self: true}}, Craft.query({:get_parallel, :a, []}, ctx.name)}
          query = {:get, :a}
          {{:read, query}, Craft.query(query, ctx.name)}
      end
    end
  end
end
