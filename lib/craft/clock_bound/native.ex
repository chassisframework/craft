defmodule Craft.ClockBound.Native do
  @moduledoc false
  @on_load :load_nif

  def load_nif do
    path = Application.app_dir(:craft, "priv/clock_wrapper")

    case :os.type() do
      # if mac os, load the NIF
      {:unix, :darwin} ->
        :erlang.load_nif(path, 0)

      _ ->
        :ok
    end
  end

  # When NIF is loaded, it will override this function.
  def get_monotonic_time, do: :erlang.nif_error(:nif_not_loaded)
end
