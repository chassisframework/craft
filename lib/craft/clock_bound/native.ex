defmodule Craft.ClockBound.Native do
  @moduledoc false
  @on_load :load_nif

  def load_nif do
    path = Application.app_dir(:craft, "priv/clock_wrapper")
    :erlang.load_nif(path, 0)
  end

  # When NIF is loaded, it will override this function.
  def get_monotonic_time, do: :erlang.nif_error(:nif_not_loaded)
end
