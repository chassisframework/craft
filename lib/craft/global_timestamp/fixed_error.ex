# A clock source with fixed error for development/testing, do not use in production.
defmodule Craft.GlobalTimestamp.FixedError do
  @moduledoc false

  alias Craft.GlobalTimestamp

  @behaviour GlobalTimestamp

  @error 3 # ms

  def now do
    now = :erlang.system_time(:nanosecond) |> DateTime.from_unix!(:nanosecond)

    earliest = DateTime.add(now, -1 * @error, :millisecond)
    latest = DateTime.add(now, @error, :millisecond)

    {:ok, %GlobalTimestamp{earliest: earliest, latest: latest}}
  end
end
