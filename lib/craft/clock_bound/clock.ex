defmodule Craft.ClockBound.Clock do
  @moduledoc false

  @behaviour Craft.ClockBound.Adapter

  alias Craft.ClockBound.Adapter
  alias Craft.ClockBound.Client

  @impl Adapter
  def monotonic_time, do: Client.monotonic_time()
end
