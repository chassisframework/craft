defmodule Craft.Nexus do
  @moduledoc """
  this module acts as a central hub for cluster messages and member event traces, as a Logger handler

  it implements a "nemesis" which can drop or delay messages programmatically

  additionally, it implements "wait until" functionality, which can block a client process until a cluster condition is met
  """
  use GenServer

  @behaviour :logger_handler

  alias Craft.Consensus
  alias Craft.GlobalTimestamp

  require Logger

  import Craft.Tracing, only: [logger_metadata: 1]

  defmodule State do
    defstruct [
      :leader,
      :lease,
      # {watcher_from, fun event, private -> :halt | {:cont, private} end, private}
      :wait_until,
      # action = :drop | {:delay, msecs} | :forward | {:forward, modified_message}
      # {fn message, private -> {action, private} end, private}
      # {fn message -> action} end, private}
      :nemesis,
      :test_process,
      members: [],
      term: -1,
      log: [],
    ]

    def leader_elected(%__MODULE__{term: term} = state, leader, new_term) when new_term > term do
      %{state | leader: leader, term: new_term}
    end

    # trace messages arrived out of order, ignore
    def leader_elected(state, _leader, _new_term), do: state

    def record_event(%__MODULE__{log: log} = state, event) do
      %{state | log: [event | log]}
    end
  end

  def start(members, test_process) do
    GenServer.start(__MODULE__, [members, test_process])
  end

  defdelegate stop(pid), to: GenServer

  # def cast(nexus, to, message) do
  #   GenServer.cast(nexus, {:cast, :os.system_time(:nanosecond), to, node(), message})
  # end

  def wait_until(nexus, condition) do
    GenServer.call(nexus, {:wait_until, condition}, 10_000)
  end

  def nemesis(nexus, fun) when is_function(fun) do
    GenServer.call(nexus, {:nemesis, fun})
  end

  # synchronously sets a nemesis and a wait condition
  def nemesis_and_wait_until(nexus, nemesis, condition) when is_function(nemesis) do
    GenServer.call(nexus, {:nemesis_and_wait_until, nemesis, condition}, 10_000)
  end

  def return_state_and_stop(nexus) do
    GenServer.call(nexus, :return_state_and_stop)
  end

  @impl GenServer
  def init([members, test_process]) do
    Logger.metadata(node: node(), nexus: self())

    {:ok, %State{members: members, test_process: test_process}}
  end

  @impl GenServer
  def handle_call({:wait_until, {module, opts}}, from, state) do
    {:noreply, %{state | wait_until: {from, &module.handle_event/2, module.init(state, opts)}}}
  end

  def handle_call({:wait_until, fun}, from, state) do
    {:noreply, %{state | wait_until: {from, fun, nil}}}
  end

  def handle_call({:nemesis, fun}, _from, state) do
    Logger.debug("nemesis activated", logger_metadata(trace: :nemesis_active))

    {:reply, :ok, %{state | nemesis: {fun, nil}}}
  end

  def handle_call({:nemesis_and_wait_until, fun, condition}, from, state) do
    {:reply, :ok, state} = handle_call({:nemesis, fun}, from, state)

    handle_call({:wait_until, condition}, from, state)
  end

  def handle_call(:return_state_and_stop, _, state) do
    {:stop, :normal, {:ok, state}, state}
  end

  @impl GenServer
  def handle_cast({:log, %{meta: %{trace: {:sent_msg, to_node, from_node, message}}} = event}, state) do
    case state.nemesis do
      {nemesis, private} ->
        {action, private} =
          case Function.info(nemesis, :arity) do
            {:arity, 1} ->
              action = nemesis.(event.meta.trace)
              {action, nil}

            {:arity, 2} ->
              nemesis.(event.meta.trace, private)
          end

        state =
          case action do
            :drop ->
              Logger.debug("dropped #{inspect message.__struct__} for #{to_node} from #{from_node}", logger_metadata(trace: {:dropped_msg, to_node, from_node, message}))
              state

            :forward ->
              Consensus.remote_operation(event.meta.name, to_node, :cast, message)
              State.record_event(state, event)

            {:forward, modified_message} ->
              event = put_in(event.meta.event, {:sent_msg, to_node, modified_message})

              Consensus.remote_operation(event.meta.name, to_node, :cast, modified_message)
              State.record_event(state, event)

            {:delay, msecs} ->
              :timer.apply_after(msecs, Consensus, :remote_operation, [event.meta.name, to_node, :cast, message])
              State.record_event(state, event)
          end

        state = %{state | nemesis: {nemesis, private}}

        {:noreply, evaluate_waiter(state, event)}

      _ ->
        Consensus.remote_operation(event.meta.name, to_node, :cast, message)

        state =
          state
          |> State.record_event(event)
          |> evaluate_waiter(event)

        {:noreply, state}
    end
  end

  def handle_cast({:log, %{meta: %{trace: {:became, :leader}, term: term, node: node}} = event}, state) do
    state =
      state
      |> State.record_event(event)
      |> State.leader_elected(node, term)
      |> evaluate_waiter(event)

    {:noreply, state}
  end

  # no lease yet
  def handle_cast({:log, %{meta: %{trace: {:quorum_reached, %{lease_expires_at: lease_expires_at}}, node: node}} = event}, %State{lease: nil} = state) do
    state =
      %{state | lease: {node, lease_expires_at}}
      |> State.record_event(event)

    {:noreply, state}
  end

  # current leaseholder extending lease
  def handle_cast({:log, %{meta: %{trace: {:quorum_reached, %{lease_expires_at: _lease_expires_at}}, node: node}} = event}, %State{lease: {node, _old_lease}} = state) do
    handle_cast({:log, event}, %{state | lease: nil})
  end

  # lease takeover, check for lease overlap
  def handle_cast({:log, %{meta: %{trace: {:quorum_reached, %{lease_expires_at: new_lease_expires_at}}, node: new_node}} = event}, state) do
    {_old_leaseholder, %GlobalTimestamp{latest: old_lease_latest}} = state.lease

    if DateTime.compare(new_lease_expires_at.earliest, old_lease_latest) != :gt do
      Process.exit(state.test_process, :lease_overlap)
    end

    state =
      %{state | lease: {new_node, new_lease_expires_at}}
      |> State.record_event(event)

    {:noreply, state}
  end

  def handle_cast({:log, %{meta: %{trace: {:became, :leader}}} = event}, state) do
    state =
      state
      |> State.record_event(event)
      |> State.leader_elected(event.meta.node, event.meta.term)
      |> evaluate_waiter(event)

    {:noreply, state}
  end

  def handle_cast({:log, event}, state) do
    {:noreply, State.record_event(state, event)}
  end

  @impl :logger_handler
  def log(%{meta: %{nexus: nexus}} = event, _config) do
    GenServer.cast(nexus, {:log, event})
  end

  def log(_event, _config), do: :noop

  defp evaluate_waiter(%State{wait_until: nil} = state, _event), do: state

  defp evaluate_waiter(%State{wait_until: {wait_from, fun, private}} = state, event) when is_function(fun, 2) do
    case fun.(event.meta.trace, private) do
      {:cont, private} ->
        %{state | wait_until: {wait_from, fun, private}}

      :halt ->
        GenServer.reply(wait_from, state)

        %{state | wait_until: nil}
    end
  end
end
