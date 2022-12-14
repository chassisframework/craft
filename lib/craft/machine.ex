defmodule Craft.Machine do
  use GenServer

  alias Craft.Consensus
  alias Craft.Consensus.FollowerState
  alias Craft.Consensus.LeaderState
  alias Craft.Log
  alias Craft.Log.Entry

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
      last_applied: 0
    ]
  end

  def name(name), do: Module.concat(__MODULE__, name)

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: name(args.name))
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
  def commit_index_bumped(%LeaderState{} = state) do
    state.name
    |> name()
    |> GenServer.cast({:commit_index_bumped, state.commit_index, state.log, state.client_requests})
  end

  def commit_index_bumped(%FollowerState{} = state) do
    state.name
    |> name()
    |> GenServer.cast({:commit_index_bumped, state.commit_index, state.log})
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

    handle_cast({:commit_index_bumped, commit_index, log}, state)
  end

  @impl true
  def handle_cast({:commit_index_bumped, new_commit_index, log}, state) do
    handle_cast({:commit_index_bumped, new_commit_index, log, nil}, state)
  end

  def handle_cast({:commit_index_bumped, new_commit_index, log, requests}, state) do
    last_applied_log_index =
      with true <- state.module.__craft_persistent__(),
           last_applied when is_integer(last_applied) <- state.module.last_applied_log_index(state.private) do
        last_applied
      else
        _ ->
          state.last_applied
      end

    private =
      Enum.reduce(last_applied_log_index..new_commit_index//1, state.private, fn index, private ->
        case Log.fetch(log, index) do
          # `nil` commands are for craft's internal use (0th log entry or log entries when a new leader is elected)
          # so we don't tell the machine about them
          {:ok, %Entry{command: nil}} ->
            private

          {:ok, %Entry{command: command}} ->
            {reply, side_effects, private} =
              case state.module.command(command, index, private) do
                {reply, private} ->
                  {reply, [], private}

                {reply, side_effects, private} ->
                  {reply, side_effects, private}
              end

            # if requests isn't nil, we're the leader
            if requests do
              case Map.fetch(requests, index) do
                {:ok, {pid, _ref} = id} ->
                  send(pid, {id, reply})

                _ ->
                  :noop
              end

              Enum.each(side_effects, fn {m, f, a} ->
                spawn(fn -> apply(m, f, a) end)
              end)
            end

            private
        end
      end)

    {:noreply, %State{state | private: private, last_applied: new_commit_index}}
  end

  defmacro __using__(opts) do
    persistent = Keyword.fetch!(opts, :persistent)
    quote do
      # FIXME: better error
      def __craft_persistent__(), do: unquote(persistent)
    end
  end
end
