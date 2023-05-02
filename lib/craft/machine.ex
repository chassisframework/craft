defmodule Craft.Machine do
  use GenServer

  alias Craft.Consensus
  alias Craft.Consensus.State, as: ConsensusState
  alias Craft.Persistence
  alias Craft.Log.CommandEntry
  alias Craft.Log.EmptyEntry
  alias Craft.Log.MembershipEntry

  @type private :: any()

  @callback init(Craft.group_name()) :: {:ok, private()}
  @callback command(Craft.command(), Craft.log_index(), private()) :: {Craft.reply(), private()} | {Craft.reply(), Craft.side_effects(), private()}
  # TODO: document that we want an int or nil if no entries have been applied
  @callback last_applied_log_index(private()) :: Craft.log_index() | nil

  defmodule State do
    defstruct [
      :name,
      :module,
      :private,
      last_applied: 0,
      client_requests: %{}
    ]
  end

  def name(name), do: Module.concat(__MODULE__, name)

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: name(args.name))
  end

  def module(%ConsensusState{} = state) do
    state.name
    |> name()
    |> GenServer.call(:module)
  end

  # FIXME: document that for persistent machines, the commit index and the
  # state mutations need to be atomically commited to the persistent store
  #
  # otherwise a crash between the two operations would result in inconsistent
  # state when the machine picks up the log where it left off
  #
  # - document that the consensus process sends the `log` state to this process,
  # and that craft assumes that the `log` is small (a handle), rather
  # than the whole log itself, the point of this is to allow the consensus process
  # to continue without being blocked by the machine process while it's applying
  # entries
  #
  def commit_index_bumped(%ConsensusState{} = state) do
    state.name
    |> name()
    |> GenServer.cast({:commit_index_bumped, state.commit_index, state.persistence, state.state})
  end

  def command(name, node, command, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    id = {self(), make_ref()}

    GenServer.cast({name(name), node}, {:command, id, command})

    receive do
      {^id, reply} ->
        reply

      after
        timeout ->
          {:error, :timeout}
    end
  end

  @impl true
  def init(args) do
    {:ok, private} = args.machine.init(args.name)

    state =
      %State{
        name: args.name,
        module: args.machine,
        private: private
      }

    {:ok, state, {:continue, :restore_machine_state}}
  end

  @impl true
  def handle_continue(:restore_machine_state, state) do
    {commit_index, log} = Consensus.catch_up(state.name)

    handle_cast({:commit_index_bumped, commit_index, log, nil}, state)
  end

  @impl true
  def handle_cast({:commit_index_bumped, new_commit_index, log, role}, state) do
    last_applied_log_index =
      with true <- state.module.__craft_persistent__(),
           last_applied when is_integer(last_applied) <- state.module.last_applied_log_index(state.private) do
        last_applied
      else
        _ ->
          state.last_applied
      end

    state =
      Enum.reduce(last_applied_log_index..new_commit_index//1, state, fn index, state ->
        case Persistence.fetch(log, index) do
          {:ok, %EmptyEntry{}} ->
            state

          {:ok, %MembershipEntry{}} ->
            state

          {:ok, %CommandEntry{command: command}} ->
            {reply, side_effects, private} =
              case state.module.command(command, index, state.private) do
                {reply, private} ->
                  {reply, [], private}

                {reply, side_effects, private} ->
                  {reply, side_effects, private}
              end

            state = %State{state | private: private}

            if role == :leader do
              Enum.each(side_effects, fn {m, f, a} ->
                spawn(fn -> apply(m, f, a) end)
              end)

              case Map.pop(state.client_requests, index) do
                {{pid, _ref} = id, client_requests} ->
                  send(pid, {id, reply})
                  %State{state | client_requests: client_requests}

                _ ->
                  state
              end
            else
              state
            end
        end
      end)

    {:noreply, %State{state | last_applied: new_commit_index}}
  end

  @impl true
  def handle_cast({:command, {from, _ref} = id, command}, state) do
    case :gen_statem.call({Consensus.name(state.name), node()}, {:machine_command, command}) do
      {:ok, index} ->
        {:noreply, %State{state | client_requests: Map.put(state.client_requests, index, id)}}

      # not leader etc
      error ->
        send(from, {id, error})
        {:noreply, state}
    end
  end

  @impl true
  def handle_call(:module, _from, state) do
    {:reply, {:ok, state.module}, state}
  end

  defmacro __using__(opts) do
    persistent = Keyword.fetch!(opts, :persistent)
    quote do
      # FIXME: better error
      def __craft_persistent__(), do: unquote(persistent)
    end
  end
end
