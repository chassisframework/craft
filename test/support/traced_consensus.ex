defmodule Craft.TracedConsensus do
  @moduledoc """
  Wrapper to capture state machine events in test

  :erlang.trace/3 can't guarantee delivery order between trace messages and messages that this process sends, so we decorate instead
  """

  @behaviour :gen_statem

  alias Craft.Consensus.State
  alias Craft.MemberCache
  alias Craft.Machine

  def start_link(args) do
    name = args.name

    args =
      case Map.pop(args, :consensus_state) do
        {nil, args} -> args
        {data, args} -> {data, args}
      end

    :gen_statem.start_link({:local, Craft.Consensus.name(name)}, __MODULE__, args, [])
  end

  defdelegate callback_mode, to: Craft.Consensus

  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [args]}
    }
  end

  def init({data, args}) when is_struct(data) do
    # forward logs to testing server
    remote_group_leader = :rpc.call(node(data.nexus_pid), Process, :whereis, [:init])
    :logger.update_process_metadata(%{gl: remote_group_leader})

    MemberCache.update(data)

    :ok = Machine.init_or_restore(data)

    state = if args[:manual_start], do: :ready_to_test, else: :lonely

    {:ok, state, data}
  end

  def init(args) do
    data = %State{
      State.new(args.name, args[:nodes], args.persistence, args.machine)
      | nexus_pid: args.nexus_pid,
        state: :lonely
    }

    init({data, args})
  end

  def ready_to_test(:enter, _, _data), do: :keep_state_and_data

  def ready_to_test(:cast, :run, %State{state: state} = data),
    do: {:next_state, state, data, []}

  def ready_to_test({:call, _from}, :catch_up, _data), do: {:keep_state_and_data, [:postpone]}

  for state <- [:lonely, :receiving_snapshot, :follower, :candidate, :leader] do
    def unquote(state)(event, msg, data) do
      send(
        data.nexus_pid,
        {DateTime.utc_now(), {:trace, node(), unquote(state), event, msg, data}}
      )

      apply(Craft.Consensus, unquote(state), [event, msg, data])
    end
  end
end
