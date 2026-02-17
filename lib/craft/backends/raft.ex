defmodule Craft.Raft do
  @moduledoc false

  alias Craft.Consensus
  alias Craft.Configuration
  alias Craft.Machine
  alias Craft.MemberCache
  alias Craft.MemberCache.GroupStatus
  alias Craft.Consensus.State.Members

  require Logger

  @doc false
  def init do
    if :net_kernel.get_state().started == :no do
      Node.start(:craft, :longnames)
    end
  end

  def start_group(name, nodes, machine, opts \\ []) do
    for node <- nodes do
      :pong = Node.ping(node)
      {:module, __MODULE__} = :rpc.call(node, Code, :ensure_loaded, [__MODULE__])
    end

    opts =
      opts
      |> Enum.into(%{})
      |> Map.merge(%{nodes: nodes, machine: machine})

    for node <- nodes do
      {:ok, _pid} = :rpc.call(node, Craft.MemberSupervisor, :start_member, [name, opts])
    end

    Craft.MemberCache.discover(name, nodes)
  end

  def stop_group(name) do
    with {:ok, %{members: members}} <- with_leader_redirect(name, &configuration(name, &1)) do
      results =
        members
        |> Members.all_nodes()
        |> Map.new(fn node ->
          result =
            try do
              :rpc.call(node, __MODULE__, :stop_member, [name])
            catch :exit, e ->
              e
            end

          {node, result}
        end)

      if Enum.all?(results, &match?({_, :ok}, &1)), do: :ok, else: results
    end
  end

  def add_member(name, node, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)

    :pong = Node.ping(node)

    case :rpc.call(node, Craft.MemberSupervisor, :start_existing_member, [name]) do
      {:error, :not_found} ->
        {:ok, config} = with_leader_redirect(name, &configuration(name, &1))

        {%{
          members: members,
          machine_module: machine_module
        }, opts} = Map.split(config, [:members, :machine_module])

        opts =
          Map.merge(opts, %{
            nodes: members.voting_nodes,
            machine: machine_module
          })

        for module <- List.flatten([__MODULE__, machine_module, opts[:global_clock] || []]) do
          {:module, ^module} = :rpc.call(node, Code, :ensure_loaded, [module])
        end

        # The nodes we provide to the new member here will eventually be overwritten when
        # the new member processes the MembershipEntry as it catches up to the leader.
        {:ok, _pid} = :rpc.call(node, Craft.MemberSupervisor, :start_member, [name, opts])

      {:ok, pid} ->
        {:ok, pid}
    end

    with_leader_redirect(name, &call_machine(name, &1, {:command, {:add_member, node}, nil}, timeout))
  end

  def remove_member(name, node, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)

    with_leader_redirect(name, &call_machine(name, &1, {:command, {:remove_member, node}, nil}, timeout))
  end

  def transfer_leadership(name, to_node) do
    with_leader_redirect(name, &Consensus.transfer_leadership(name, &1, to_node))
  end

  def transfer_leadership(name) do
    with_leader_redirect(name, &Consensus.transfer_leadership(name, &1))
  end

  def backup(name, path) do
    File.mkdir_p!(path)

    with :ok <- Configuration.copy_configuration(name, path),
         :ok <- Consensus.backup(name, path) do
      :ok
    else
      error ->
        error
    end
  end

  def restore(path) do
    config =
      path
      |> Configuration.configuration_file()
      |> Configuration.read_file()

    if Craft.MemberSupervisor.member_running?(config.name) do
      raise "unable to restore, local member for group #{config.name} is running, you must first stop it with Craft.stop_member/1"
    end

    Configuration.delete_member_data(config.name)
    Configuration.restore_from_backup(path)

    :ok
  end

  def send(name, message) do
    if pid = Craft.Application.lookup(name, Craft.Machine) do
      Kernel.send(pid, message)

      :ok
    else
      {:error, :unknown_group}
    end
  end

  def reply_from, do: :not_implemented

  def reply({:direct, query_from}, reply) do
    GenServer.reply(query_from, reply)
  end

  def reply({:quorum, query_time, machine_pid, query_from}, reply) do
    GenServer.call(machine_pid, {{:query_reply, query_time, reply}, query_from})
  end

  def start_member(name) do
    case Craft.MemberSupervisor.start_existing_member(name) do
      {:error, :not_found} ->
        {:ok, config} = with_leader_redirect(name, &configuration(name, &1))

        {%{
          members: members,
          machine_module: machine_module
        }, opts} = Map.split(config, [:members, :machine_module])

        opts =
          Map.merge(opts, %{
            nodes: members.voting_nodes,
            machine: machine_module
          })

        for module <- List.flatten([__MODULE__, machine_module, opts[:global_clock] || []]) do
          {:module, ^module} = Code.ensure_loaded(module)
        end

        # The nodes we provide to the new member here will eventually be overwritten when
        # the new member processes the MembershipEntry as it catches up to the leader.
        Craft.MemberSupervisor.start_member(name, opts)

      {:ok, pid} ->
        {:ok, pid}
    end
  end

  defdelegate stop_member(name), to: Craft.MemberSupervisor
  defdelegate discover(name, nodes), to: MemberCache
  defdelegate holding_lease?(name), to: MemberCache
  defdelegate leader_ready?(name), to: MemberCache
  defdelegate holding_lease?(), to: Machine
  defdelegate known_groups(), to: MemberCache, as: :all
  defdelegate cached_info(group_name), to: MemberCache, as: :get
  defdelegate purge(name), to: Configuration, as: :delete_member_data
  defdelegate now(), to: Machine

  def command(command, name, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)

    with_leader_redirect(name, &call_machine(name, &1, {:command, {:machine_command, command}, nil}, timeout))
  end

  def async_command(command, name, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)

    with_leader_redirect(name, &call_machine(name, &1, {:command, {:machine_command, command}, self()}, timeout))
  end

  def query(query, name, opts \\ []) do
    consistency = Keyword.get(opts, :consistency, :linearizable)
    timeout = Keyword.get(opts, :timeout, 5_000)

    case consistency do
      :linearizable ->
        with_leader_redirect(name, &call_machine(name, &1, {:query, :linearizable, query}, timeout))

      {:eventual, :leader} ->
        with_leader_redirect(name, &call_machine(name, &1, {:query, {:eventual, :leader}, query}, timeout))

      {:eventual, {:node, node}} ->
        call_machine(name, node, {:query, :eventual, query}, timeout)

      :eventual ->
        case MemberCache.get(name) do
          {:ok, %GroupStatus{} = group_status} ->
            node =
              group_status.members
              |> Map.keys()
              |> Enum.random()

            call_machine(name, node, {:query, :eventual, query}, timeout)

          :not_found ->
            Logger.error("No known nodes for group '#{inspect(name)}', have you called Craft.discover/2?")

            {:error, :unknown_group}
        end
    end
  end

  def step_down(name) do
    with_leader_redirect(name, &Consensus.step_down(name, &1))
  end

  def switch_mode(name, mode, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)

    case MemberCache.get(name) do
      {:ok, %GroupStatus{} = group_status} ->
        results =
          group_status.members
          |> Map.keys()
          |> Map.new(fn node ->
            {node, call_machine(name, node, {:switch_mode, mode}, timeout)}
          end)

        if Enum.all?(results, fn {_node, result} -> result == :ok end) do
          :ok
        else
          results
        end

      :not_found ->
        Logger.error("No known nodes for group '#{inspect(name)}', have you called Craft.discover/2?")

        {:error, :unknown_group}
    end
  end

  defp with_leader_redirect(name, func) do
    case MemberCache.get(name) do
      {:ok, %GroupStatus{} = group_status} ->
        members =
          group_status.members
          |> Map.keys()
          |> MapSet.new()

        if group_status.leader do
          do_leader_redirect(name, group_status.leader, members, func)
        else
          do_leader_redirect(name, Enum.random(members), members, func)
        end

      :not_found ->
        Logger.error("No known nodes for group '#{inspect(name)}', have you called Craft.discover/2?")

        {:error, :unknown_group}
    end
  end

  defp do_leader_redirect(name, leader, members, func, previous_redirects \\ MapSet.new()) do
    case func.(leader) do
      {:error, error} when error in [:unknown_leader, :nodedown] ->
        members = MapSet.delete(members, leader)

        if Enum.empty?(members) do
          {:error, :unknown_leader}
        else
          do_leader_redirect(name, Enum.random(members), members, func)
        end

      {:error, {:not_leader, leader}} ->
        if MapSet.member?(previous_redirects, leader) do
          {:error, :redirect_loop}
        else
          MemberCache.update_leader(name, leader)

          do_leader_redirect(name, leader, members, func, MapSet.put(previous_redirects, leader))
        end

      reply ->
        reply
    end
  end

  def state(name, node) do
    try do
      {node,
       consensus: Consensus.state(name, node),
       machine: Machine.state(name, node)}
    catch :exit, e ->
      {node, e}
    end
  end

  def state(name) do
    {:ok, %{members: members}} = with_leader_redirect(name, &configuration(name, &1))

    members.voting_nodes
    |> MapSet.union(members.non_voting_nodes)
    |> Enum.into(%{}, &state(name, &1))
  end

  def call_machine(name, node, request, timeout) do
    case Machine.call(name, node, request, timeout) do
      {:badrpc, {:EXIT, {reason, _}}} ->
        {:error, reason}

      {:badrpc, reason} ->
        {:error, reason}

      result ->
        result
    end
  end

  defp configuration(name, node) do
    try do
      Consensus.configuration(name, node)
    catch :exit, e ->
      {:error, e}
    end
  end
end
