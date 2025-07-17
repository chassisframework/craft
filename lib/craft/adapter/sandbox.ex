defmodule Craft.Adapter.Sandbox do
  @moduledoc false

  use GenServer

  alias Craft.Adapter.Local

  @custom_lookup Application.compile_env(:craft, __MODULE__, [])[:custom_lookup]

  # Passthrough
  for {func, arity} <- [discover: 2, transfer_leadership: 2, add_member: 2, holding_lease?: 1, cached_info: 1] do
    args = Macro.generate_arguments(arity, __MODULE__)

    def unquote(func)(unquote_splicing(args)) do
      Local.unquote(func)(unquote_splicing(args))
    end
  end

  # Public API
  def start_member(group) do
    instance = lookup!()
    Local.start_member(group <> proc_name(instance), instance)
  catch
    :exit, {:noproc, _} -> :ok
  end

  def start_group(group, nodes, machine, opts \\ []) do
    instance = lookup!()
    Local.start_group(group <> proc_name(instance), nodes, machine, opts, [:in_memory], instance)
  end

  def command(command, group, opts) do
    instance = lookup!()
    Local.command(command, group <> proc_name(instance), opts, instance)
  end

  def query(query, group, opts) do
    instance = lookup!()
    Local.query(query, group <> proc_name(instance), opts, instance)
  end

  def send(group, message) do
    instance = lookup!()
    Local.send(group <> proc_name(instance), message, instance)
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def checkout(checkout_pid \\ self(), opts \\ []) do
    if not is_nil(GenServer.call(__MODULE__, {:lookup, []})) do
      raise "Sandbox already checked out for this process"
    end

    {:ok, store_pid} = Local.start(Keyword.put(opts, :name, nil))
    :ok = GenServer.call(__MODULE__, {:register, checkout_pid, store_pid})

    {:ok, store_pid}
  end

  def allow(pid_or_identifier) do
    GenServer.call(__MODULE__, {:allow, pid_or_identifier})
  end

  @impl GenServer
  def init(_args) do
    {:ok, %{by_caller: %{}, by_store: %{}}}
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    if store_pid = Map.get(state.by_caller, pid) do
      state = drop(state, pid, store_pid)

      if is_nil(state.by_store[store_pid]) do
        GenServer.stop(store_pid)
      end

      {:noreply, state}
    else
      {:noreply, state}
    end
  end

  @impl GenServer
  def handle_call({:register, caller_pid, store_pid}, _from, state) do
    Process.monitor(caller_pid)

    {:reply, :ok, put(state, caller_pid, store_pid)}
  end

  @impl GenServer
  def handle_call({:allow, allowed}, {caller_pid, _}, state) when is_pid(allowed) do
    Process.monitor(allowed)
    store_pid = Map.get(state.by_caller, caller_pid)

    {:reply, :ok, put(state, allowed, store_pid)}
  end

  @impl GenServer
  def handle_call({:allow, allowed}, {caller_pid, _}, state) do
    store_pid = Map.get(state.by_caller, caller_pid)

    {:reply, :ok, put(state, allowed, store_pid)}
  end

  @impl GenServer
  def handle_call({:lookup, callers}, {caller_pid, _}, state) do
    pids = [caller_pid | callers]

    response =
      Enum.reduce_while(pids, nil, fn pid, acc ->
        case Map.get(state.by_caller, pid) do
          store_pid when is_pid(store_pid) -> {:halt, store_pid}
          nil -> {:cont, acc}
        end
      end)

    {response, state} =
      if is_nil(response) && @custom_lookup do
        Enum.reduce_while(pids, nil, fn pid, _acc ->
          case @custom_lookup.lookup(pid) do
            nil ->
              {:cont, {nil, state}}

            custom_identifier ->
              if store_pid = Map.get(state.by_caller, custom_identifier) do
                Process.monitor(pid)
                {:halt, {store_pid, state |> put(pid, store_pid) |> drop(custom_identifier, store_pid)}}
              else
                {:halt, {nil, state}}
              end
          end
        end)
      else
        {response, state}
      end

    {:reply, response, state}
  end

  defp lookup! do
    case GenServer.call(__MODULE__, {:lookup, Process.get(:"$callers", [])}) do
      nil ->
        raise """
        No Authz.Store sandbox checked out for this process or any of it's $callers.

        Try using Authz.Store.Sandbox.allow(other_pid) to register another pid with a checked out sandbox.
        """

      instance ->
        instance
    end
  end

  defp proc_name(pid) do
    pid |> :erlang.pid_to_list() |> Enum.slice(1..-2//1) |> to_string()
  end

  defp put(%{by_caller: by_caller, by_store: by_store} = state, caller_pid, store_pid) do
    %{
      state
      | by_caller: Map.put(by_caller, caller_pid, store_pid),
        by_store: Map.update(by_store, store_pid, MapSet.new([caller_pid]), &MapSet.put(&1, caller_pid))
    }
  end

  defp drop(%{by_caller: by_caller, by_store: by_store} = state, caller_pid, store_pid) do
    state = %{
      state
      | by_caller: Map.delete(by_caller, caller_pid),
        by_store: Map.update(by_store, store_pid, MapSet.new([]), &MapSet.delete(&1, caller_pid))
    }

    if state.by_store |> Map.get(store_pid) |> MapSet.size() == 0 do
      Map.update!(state, :by_store, &Map.delete(&1, store_pid))
    else
      state
    end
  end
end
