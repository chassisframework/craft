defmodule Craft.Sandbox do
  @moduledoc """
  Non-distributed testing backend, isolates groups into namespaces. Suitable for testing basic functionality of an application state machine.

  In full production mode (i.e. using the Craft.Raft backend), each application state machine runs in its own process, when using the sandbox,
  all machines run in a single sandbox process. So user functionality that expects process isolation will not operate correctly.

  Usage:

  Explicit mode:
    `config :craft, :backend, Craft.Sandbox`

    then, in your test code, join a sandbox:
    ```
    setup do
      Craft.Sandbox.join("my sandbox", pid \\ self())`
    end
    ```

  Implicit mode:
    Adding the `:lookup` key enables implicit mode, the sandbox is determined by calling the `{m, f}` provided with the pid as its sole argument.

    `config :craft, :backend, {Craft.Sandbox, lookup: {m, f}}`

  Shared mode, single global sandbox (useful for dev):
    `config :craft, :backend, {Craft.Sandbox, mode: :shared}`

  Inheritance can be enabled by setting the `:mode` to `:inherit`:
    `config :craft, :backend, {Craft.Sandbox, mode: :inherit, lookup: {m, f}}`

    Inheritance allows the test process to inherit the sandbox of 
      1) any process defined in the $callers entry of its process dictionary (i.e. the process that calls TaskSupervisor.async) or 
      2) the ancestors that spawned the process
  """
  use GenServer

  alias Craft.Configuration

  import Craft.Application, only: [via: 2, lookup: 2]

  # test pid -> sandbox name
  opts =
    case Application.compile_env(:craft, :backend) do
      {__MODULE__, opts} -> opts
      _ -> []
    end

  if opts[:mode] == :shared do
    defp do_find_sandbox(_pid), do: {:ok, :shared}
  else
    with {m, f} <- opts[:lookup] do
      defp do_find_sandbox(pid) do
        case :erlang.apply(unquote(m), unquote(f), [pid]) do
          {:ok, name} ->
            DynamicSupervisor.start_child(Craft.Supervisor, {Craft.Sandbox, name})
            {:ok, name}

          error ->
            error
        end
      end
    else
      _ ->
        defp do_find_sandbox(pid), do: __MODULE__.Manager.find(pid)
    end
  end

  if opts[:mode] == :inherit do
    defp find_sandbox() do
      self()
      |> do_find_sandbox()
      |> find_sandbox_by_callers()
      |> find_sandbox_by_parents()
    end

    defp find_sandbox_by_callers({:ok, name}), do: {:ok, name}

    defp find_sandbox_by_callers(:error) do
      caller_pids = Process.get(:"$callers", [])

      Enum.reduce_while(caller_pids, :error, fn pid, _acc ->
        case do_find_sandbox(pid) do
          {:ok, name} -> {:halt, {:ok, name}}
          :error -> {:cont, :error}
        end
      end)
    end

    defp find_sandbox_by_parents({:ok, name}), do: {:ok, name}

    defp find_sandbox_by_parents(:error),
      do: find_sandbox_on_next_parent(Process.info(self(), :parent))

    defp find_sandbox_on_next_parent({:parent, pid}) when is_pid(pid) do
      # if the ancestor is a sandbox, return it
      # this happens when the machine spawns a process
      {:dictionary, dictionary} = Process.info(pid, :dictionary)

      if name = dictionary[:__CRAFT_SANDBOX__] do
        {:ok, name}
      else
        case do_find_sandbox(pid) do
          {:ok, name} -> {:ok, name}
          :error -> find_sandbox_on_next_parent(Process.info(pid, :parent))
        end
      end
    end

    defp find_sandbox_on_next_parent(_no_parent), do: :error
  else
    defp find_sandbox(), do: do_find_sandbox(self())
  end

  def init do
    with {__MODULE__, opts} <- Application.get_env(:craft, :backend),
         :shared <- opts[:mode] do
      {:ok, _} = DynamicSupervisor.start_child(Craft.Supervisor, {Craft.Sandbox, :shared})
    end
  end

  @doc "Enters the specified process into the sandbox given by `name`"
  def join(name, pid \\ self()) do
    GenServer.call(__MODULE__.Manager, {:join, name, pid})
  end

  @doc "Stops the sandbox, destroying all machine state."
  def stop(name) do
    if pid = lookup(name, Craft.Sandbox) do
      DynamicSupervisor.terminate_child(Craft.Supervisor, pid)
    end
  end

  @doc false
  def start_group(name, nodes, machine, opts) do
    GenServer.call(find!(), {:start_group, name, nodes, machine, opts})
  end

  @doc false
  def stop_group(name) do
    GenServer.call(find!(), {:stop_member, name})
  end

  @doc false
  def start_member(name, opts) do
    GenServer.call(find!(), {:start_member, name, opts})
  end

  @doc false
  def stop_member(name) do
    GenServer.call(find!(), {:stop_member, name})
  end

  @doc false
  def command(command, name, _opts) do
    GenServer.call(find!(), {:command, name, command})
  end

  @doc false
  def query(query, name, _opts) do
    GenServer.call(find!(), {:query, name, query})
  end

  @doc false
  def send(name, message) do
    Kernel.send(find!(), {:user_message, name, message})
  end

  def reply({:direct, {_pid, _ref} = reply_ref, sandbox_pid}, reply) do
    if find!() != sandbox_pid do
      raise "sandbox boundary violation"
    end

    GenServer.reply(reply_ref, reply)
  end

  def reply({:quorum, _query_time, _machine_pid, _query_from}, _reply), do: :not_implemented

  @doc false
  def backup(name, path) do
    GenServer.call(find!(), {:backup, name, path})
  end

  @doc false
  def restore(path) do
    GenServer.call(find!(), {:restore, path})
  end

  @doc false
  def now do
    Craft.GlobalTimestamp.FixedError.now()
  end

  @doc false
  def holding_lease?(_name), do: true
  @doc false
  def holding_lease?(), do: true

  @doc false
  def known_groups(), do: :not_implemented
  @doc false
  def add_member(_name, _node, _opts), do: :not_implemented
  @doc false
  def remove_member(_name, _node, _opts), do: :not_implemented
  @doc false
  def transfer_leadership(_name), do: :not_implemented
  @doc false
  def transfer_leadership(_name, _to_node), do: :not_implemented
  @doc false
  def discover(_name, _nodes), do: :not_implemented
  @doc false
  def step_down(_name), do: :not_implemented
  @doc false
  def cached_info(_name), do: :not_implemented
  @doc false
  def purge(_name), do: :not_implemented
  @doc false
  def state(_name), do: :not_implemented
  @doc false
  def state(_name, _node), do: :not_implemented

  defp find!() do
    # if the sandbox itself is the caller, it's its own sandbox
    if Process.get(:__CRAFT_SANDBOX__) do
      self()
    else
      with {:ok, name} <- find_sandbox(),
           sandbox_pid when is_pid(sandbox_pid) <- lookup(name, Craft.Sandbox) do
        sandbox_pid
      else
        _ ->
          raise "no sandbox configured for pid #{inspect(self())}"
      end
    end
  end

  def current_index(name, group) do
    GenServer.call(via(name, __MODULE__), {:current_index, group})
  end

  @doc false
  def start_link(name) do
    GenServer.start_link(__MODULE__, name, name: via(name, __MODULE__))
  end

  defmodule MachineState do
    defstruct [
      :module,
      :private,
      index: 0
    ]
  end

  @impl GenServer
  def init(name) do
    Process.put(:__CRAFT_SANDBOX__, name)

    {:ok, %{}}
  end

  @impl GenServer
  def handle_call({:current_index, group}, _from, state) do
    case state[group] do
      nil -> {:reply, {:error, :group_not_started}, state}
      machine_state -> {:reply, {:ok, machine_state.index}, state}
    end
  end

  @impl GenServer
  def handle_call({:start_group, name, nodes, machine, opts}, _from, state) do
    if state[name] do
      {:reply, {:error, :already_started}, state}
    else
      if !Configuration.find(name, namespace()) do
        config = %{
          machine: machine,
          nodes: nodes,
          global_clock: opts[:global_clock]
        }

        Configuration.write_new!(name, config, namespace())
      end

      {:reply, :ok, init_machine(name, machine, state)}
    end
  end

  def handle_call({:start_member, name, _opts}, _from, state) do
    if state[name] do
      {:reply, {:error, :already_started}, state}
    else
      if config = Configuration.find(name, namespace()) do
        {:reply, :ok, init_machine(name, config.machine, state)}
      else
        {:reply, {:error, :unknown_group}, state}
      end
    end
  end

  def handle_call({:stop_member, name, _opts}, _from, state) do
    if state[name] do
      {:reply, :ok, Map.delete(state, name)}
    else
      {:reply, {:error, :unknown_group}, state}
    end
  end

  def handle_call({:command, name, command}, _from, state) do
    if machine_state = state[name] do
      {reply, private} =
        machine_state.module.handle_command(command, machine_state.index, machine_state.private)

      {:reply, reply,
       Map.put(state, name, %{machine_state | private: private, index: machine_state.index + 1})}
    else
      {:reply, {:error, :unknown_group}, state}
    end
  end

  def handle_call({:query, name, query}, from, state) do
    if machine_state = state[name] do
      case machine_state.module.handle_query(
             query,
             {:direct, from, self()},
             machine_state.private
           ) do
        {:reply, response} ->
          {:reply, response, state}

        :noreply ->
          {:noreply, state}
      end
    else
      {:reply, {:error, :unknown_group}, state}
    end
  end

  def handle_call({:backup, name, path}, state) do
    if machine_state = state[name] do
      machine_state.backup(path, machine_state.private)

      {:reply, :ok, state}
    else
      {:reply, {:error, :unknown_group}, state}
    end
  end

  def handle_call({:restore, path}, state) do
    config =
      path
      |> Configuration.configuration_file()
      |> Configuration.read_file()

    if state[config.name] do
      raise "unable to restore, local member for group #{config.name} is running, you must first stop it with Craft.stop_member/1"
    end

    Configuration.delete_member_data(config.name)
    Configuration.restore_from_backup(path)

    {:reply, :ok}
  end

  @impl GenServer
  def handle_info({:user_message, name, message}, state) do
    if machine_state = state[name] do
      private = machine_state.module.handle_info(message, machine_state.private)

      {:noreply, Map.put(state, name, %{machine_state | private: private})}
    else
      {:noreply, state}
    end
  end

  defp init_machine(name, machine, state) do
    args =
      if machine.__craft_mutable__() do
        data_dir =
          name
          |> Configuration.find(namespace())
          |> Map.fetch!(:data_dir)

        data_dir = Path.join([Configuration.data_dir(), data_dir, "machine"])

        File.mkdir_p!(data_dir)

        %{name: name, data_dir: data_dir}
      else
        %{name: name}
      end

    {:ok, private} = machine.init(args)

    Map.put(state, name, %MachineState{module: machine, private: private})
  end

  defp namespace do
    self()
    |> :erlang.pid_to_list()
    |> :erlang.list_to_binary()
    |> Base.encode16()
  end

  defmodule Manager do
    @moduledoc false

    use GenServer

    import Craft.Application, only: [lookup: 2]

    def start_link(args) do
      GenServer.start_link(__MODULE__, args, name: __MODULE__)
    end

    def find(pid) do
      GenServer.call(__MODULE__, {:find, pid})
    end

    defmodule State do
      defstruct name_to_pids: %{},
                pid_to_name: %{}
    end

    def init(_) do
      {:ok, %State{}}
    end

    def handle_call({:join, name, pid}, _from, state) do
      if current = state.pid_to_name[pid] do
        {:reply, {:error, {:already_joined, current}}, state}
      else
        pids =
          case Map.fetch(state.name_to_pids, name) do
            {:ok, pids} ->
              MapSet.put(pids, pid)

            :error ->
              {:ok, _} = DynamicSupervisor.start_child(Craft.Supervisor, {Craft.Sandbox, name})

              MapSet.new([pid])
          end

        state =
          %{
            state
            | name_to_pids: Map.put(state.name_to_pids, name, pids),
              pid_to_name: Map.put(state.pid_to_name, pid, name)
          }

        Process.monitor(pid)
        {:reply, :ok, state}
      end
    end

    def handle_call({:find, pid}, _from, state) do
      {:reply, Map.fetch(state.pid_to_name, pid), state}
    end

    def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
      state =
        case Map.pop(state.pid_to_name, pid) do
          {nil, _} ->
            state

          {name, pid_to_name} ->
            {_, name_to_pids} =
              Map.get_and_update!(state.name_to_pids, name, fn pids ->
                if (sandbox = lookup(name, Craft.Sandbox)) && pids == MapSet.new([pid]) do
                  DynamicSupervisor.terminate_child(Craft.Supervisor, sandbox)

                  :pop
                else
                  {nil, MapSet.delete(pids, pid)}
                end
              end)

            %{state | name_to_pids: name_to_pids, pid_to_name: pid_to_name}
        end

      {:noreply, state}
    end
  end
end
