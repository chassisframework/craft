defmodule Craft.ClockBound.Adapter do
  @moduledoc """
  Interface for the ClockBound.
  """
  @doc "See `Craft.ClockBound.Client.monotonic_time/0`."
  @callback monotonic_time() :: integer()
end
