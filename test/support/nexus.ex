defmodule Craft.Nexus do
  @moduledoc """
  this module acts as a central hub for cluster messages and member traces

  it implements a "nemesis" which can drop or delay messages programmatically

  additionally, it implements "wait until" functionality, which can block a client process until a cluster condition is met
  """
  use GenServer

  alias Craft.Consensus.State, as: ConsensusState

  defmodule State do
    defstruct [
      members: [],
      term: -1,
      leader: nil,
      events: [],
      # {watcher_from, fun event, private -> :halt | {:cont, private} end, private}
      wait_until: nil,
      # action = :drop | {:delay, msecs} | :forward | {:forward, modified_message}
      # {fn message, private -> {action, private} end, private}
      nemesis: nil
    ]

    def leader_elected(%State{term: term} = state, leader, new_term) when new_term > term do
      %__MODULE__{state | leader: leader, term: new_term}
    end
  end

  def start_link(members) do
    GenServer.start_link(__MODULE__, members)
  end

  defdelegate stop(pid), to: GenServer

  def cast(nexus, to, message) do
    GenServer.cast(nexus, {:cast, to, node(), message})
  end

  def wait_until(nexus, condition) do
    GenServer.call(nexus, {:wait_until, condition}, 10_000)
  end

  def nemesis(nexus, fun) when is_function(fun, 2) do
    GenServer.call(nexus, {:nemesis, fun})
  end

  # synchronously sets a nemesis and a wait condition
  def nemesis_and_wait_until(nexus, nemesis, condition) when is_function(nemesis, 2) do
    GenServer.call(nexus, {:nemesis_and_wait_until, nemesis, condition}, 10_000)
  end


  def init(members) do
    {:ok, %State{members: members}}
  end

  def handle_call({:wait_until, {module, opts}}, from, state) do
    {:noreply, %State{state | wait_until: {from, &module.handle_event/2, module.init(state, opts)}}}
  end

  def handle_call({:wait_until, fun}, from, state) do
    {:noreply, %State{state | wait_until: {from, fun, nil}}}
  end

  def handle_call({:nemesis, fun}, _from, state) do
    {:reply, :ok, %State{state | nemesis: {fun, nil}}}
  end

  def handle_call({:nemesis_and_wait_until, fun, condition}, from, state) do
    {:reply, :ok, state} = handle_call({:nemesis, fun}, from, state)

    handle_call({:wait_until, condition}, from, state)
  end

  def handle_cast({:cast, to, from, message}, state) do
    {_, to_node} = to
    event = {:cast, to_node, from, message}

    state = evaluate_waiter(state, event)

    state =
      case state.nemesis do
        {nemesis, private} ->
          {action, private} = nemesis.(event, private)

          case action do
            :drop ->
              :noop

            :forward ->
              :gen_statem.cast(to, message)

            {:forward, modified_message} ->
              :gen_statem.cast(to, modified_message)

            {:delay, msecs} ->
              :timer.apply_after(msecs, :gen_statem, :cast, [to, message])
          end

          %State{state | nemesis: {nemesis, private}}

        _ ->
          :gen_statem.cast(to, message)

          state
      end

    {:noreply, state}
  end

  def handle_info({:trace, _time, from, :leader, :enter, :candidate, %ConsensusState{current_term: current_term}} = event, state) do
    state =
      state
      |> State.leader_elected(from, current_term)
      |> evaluate_waiter(event)

    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp evaluate_waiter(%State{wait_until: nil} = state, _event), do: state

  defp evaluate_waiter(%State{wait_until: {wait_from, fun, private}} = state, event) when is_function(fun, 2) do
    case fun.(event, private) do
      {:cont, private} ->
        %State{state | wait_until: {wait_from, fun, private}}

      :halt ->
        GenServer.reply(wait_from, state)

        %State{state | wait_until: nil}
    end
  end
end
