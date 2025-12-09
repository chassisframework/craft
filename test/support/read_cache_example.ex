defmodule Craft.ReadCacheExample do
  @moduledoc """
  This is an example read-cache that frontends a raft group, using pipelined (async) writes for speed.

  Usage:

  Craft.ReadCacheExample.start_link()
  Craft.ReadCacheExample.check_linearizability()
  """

  use GenServer

  alias Craft.ParallelClients
  alias Craft.Linearizability
  alias Craft.Linearizability.TestModel

  def check_linearizability do
    reset()

    history = ParallelClients.run(random_request_fun(), _num_client = 10, _num_commands = 20, nil)

    case Linearizability.linearize(history, __MODULE__) do
      {:ok, _linearied_history, _ignored_ops} ->
        :ok

      error ->
        error
    end
  end

  def start_link(args \\ nil) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def put(k, v) do
    GenServer.call(__MODULE__, {:put, k, v})
  end

  def get(k) do
    GenServer.call(__MODULE__, {:get, k})
  end

  def reset do
    GenServer.call(__MODULE__, :reset)
  end

  @impl GenServer
  def init(nil) do
    {:ok, nil, {:continue, :start_group}}
  end

  @impl GenServer
  def handle_continue(:start_group, nil) do
    nodes = Craft.TestCluster.spawn_nodes(5)

    :ok = Craft.start_group(__MODULE__, nodes, Craft.SimpleMachine, global_clock: Craft.GlobalTimestamp.FixedError)

    Craft.MemberCache.discover(__MODULE__, nodes)

    {:noreply, {_cache = %{}, _async_ops = %{}}}
  end

  @impl GenServer
  def handle_call(:reset, _from, {_cache, _async_ops}) do
    {:reply, :ok, {%{}, %{}}}
  end

  def handle_call({:get, k}, _from, {cache, _async_ops} = state) when is_map_key(cache, k) do
    {:reply, {:ok, cache[k]}, state}
  end
  def handle_call({:get, _k}, _from, state), do: {:reply, {:error, :not_found}, state}

  def handle_call(operation, from, {cache, async_ops}) do
    # sync version
    # Craft.command(operation, __MODULE__)
    # {:reply, :ok, {cache, async_ops}}

    {:ok, ref} = Craft.async_command(operation, __MODULE__)

    {:noreply, {cache, Map.put(async_ops, ref, {from, operation})}}
  end

  @impl GenServer
  def handle_info({:"$craft_command", ref, :ok}, {cache, async_ops}) when is_map_key(async_ops, ref) do
    {{from, operation}, async_ops} = Map.pop(async_ops, ref)

    cache =
      case operation do
        {:put, k, v} ->
          Map.put(cache, k, v)
      end

    GenServer.reply(from, :ok)

    {:noreply, {cache, async_ops}}
  end
  
  @behaviour TestModel

  @impl TestModel
  def init, do: {:ok, %{}}

  @impl TestModel
  def write({:put, k, v}, state), do: {:ok, Map.put(state, k, v)}

  @impl TestModel
  def read({:get, k}, state), do: {:ok, Map.get(state, k)}

  defp random_request_fun do
    fn i ->
      value =
        self()
        |> :erlang.pid_to_list()
        |> :erlang.list_to_binary()
        |> String.trim("<")
        |> String.trim(">")

      case :rand.uniform(100) do
        val when val > 50 ->
          k = :a
          v = "#{value}_#{i}"
          {{:write, {:put, k, v}}, put(k, v)}

        _ ->
          k = :a
          {{:read, {:get, k}}, get(k)}
      end
    end
  end
end
