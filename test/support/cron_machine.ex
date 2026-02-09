defmodule Craft.CronMachine do
  use Craft.Machine, mutable: false

  # @run_at ~T[00:00:00]
  @run_at Time.utc_now() |> Time.add(20, :second)

  @check_interval :timer.seconds(5)

  def run(group_name) do
    Craft.command(:run, group_name)
  end

  defmodule State do
    defstruct [:group_name, :timer, :next_run_at]
  end

  @impl true
  def init(args) do
    {:ok, %State{group_name: args.group_name, next_run_at: next_run_at()}}
  end

  @impl true
  def handle_command(:run, _log_index, state) do
    if Craft.holding_lease?() do
      spawn(fn ->
        IO.puts "ran!"
      end)
    end

    {:ok, %{state | next_run_at: next_run_at()}}
  end

  @impl true
  def handle_query(_, _from, state), do: {:reply, :not_implemented, state}

  @impl true
  def handle_role_change(:leader, state) do
    {:ok, ref} = :timer.send_after(@check_interval, self(), :check)

    %{state | timer: ref}
  end

  def handle_role_change(:follower, state) do
    :timer.cancel(state.timer)

    %{state | timer: nil}
  end
  def handle_role_change(_, state), do: state

  @impl true
  def handle_info(:check, state) do
    {:ok, ref} = :timer.send_after(@check_interval, self(), :check)

    if DateTime.compare(state.next_run_at, now()) == :lt do
      spawn_link(__MODULE__, :run, [state.group_name])
    end

    %{state | timer: ref}
  end

  @impl true
  def receive_snapshot(snapshot, _state), do: snapshot

  @impl true
  def snapshot(state), do: state

  def dump(state), do: state

  defp next_run_at(time \\ @run_at) do
    now = Time.utc_now()

    seconds_until_run =
      if Time.after?(time, now) do
        Time.diff(time, now, :second)
      else
        Time.diff(time, now, :second) + (24 * 60 * 60)
      end

    DateTime.add(now(), seconds_until_run, :second)
  end

  defp now do
    {:ok, %Craft.GlobalTimestamp{latest: latest}} = Craft.now()

    latest
  end
end
