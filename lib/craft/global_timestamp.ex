defmodule Craft.GlobalTimestamp do
  @moduledoc """
  Behaviour specifying a global clock with bounded error.
  """

  defstruct [:earliest, :latest]

  @type t :: %__MODULE__{
    earliest: DateTime.t(),
    latest: DateTime.t()
  }

  @callback now() :: {:ok, t()} | {:error, :try_again} | :error

  def now(nil), do: {:ok, nil}
  def now(global_clock), do: global_clock.now()

  def add(%__MODULE__{} = ts, amount, unit) do
    %__MODULE__{
      earliest: DateTime.add(ts.earliest, amount, unit),
      latest: DateTime.add(ts.latest, amount, unit)
    }
  end

  def time_until_lease_expires(global_clock, lease_expires_at) do
    case now(global_clock) do
      {:ok, %__MODULE__{earliest: now_earliest}} ->
        # if the lease has already expired, report 0
        {:ok, max(0, DateTime.diff(lease_expires_at.latest, now_earliest, :millisecond))}

      _ ->
        :error
    end
  end
end
