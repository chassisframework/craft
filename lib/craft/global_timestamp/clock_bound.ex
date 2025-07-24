defmodule Craft.GlobalTimestamp.ClockBound do
  @moduledoc """
    Client for AWS' ClockBound daemon, which provides global clock error bounds via `chronyd`.

    https://github.com/aws/clock-bound
    https://github.com/aws/clock-bound/blob/main/docs/PROTOCOL.md
  """

  alias Craft.GlobalTimestamp

  @on_load :load_nif
  @default_mem_path "/var/run/clockbound/shm0"

  # all this silly indirection is just to make testing easier
  def now(read_fun \\ fn -> File.read(Application.get_env(:craft, :clock_bound_shm_path, @default_mem_path)) end, retries \\ 1_000) do
    with {:ok, binary} <- read_fun.(),
         {:retry, reason} <- do_now(binary) do
      if retries > 0 do
        now(read_fun, retries - 1)
      else
        {:error, reason}
      end
    else
      ok_or_error ->
        ok_or_error
    end
  end

  def do_now(
        <<0x414D5A4E :: unsigned-native-32,
          0x43420200 :: unsigned-native-32,
          segment_size :: unsigned-native-32,
          version :: unsigned-native-16,
          generation :: unsigned-native-16,
          as_of_sec :: signed-native-64,
          as_of_nsec :: signed-native-64,
          void_after_sec :: signed-native-64,
          void_after_nsec :: signed-native-64,
          bound :: signed-native-64,
          _disruption_marker :: unsigned-native-64,
          max_drift :: unsigned-native-32,
          clock_status :: signed-native-32,
          _clock_disruption_support :: unsigned-native-8,
          _padding :: binary>> = binary) do
    as_of = timespec_to_nanosecond({as_of_sec, as_of_nsec})
    void_after = timespec_to_nanosecond({void_after_sec, void_after_nsec})

    # sandwich the wall-clock call between two montotonic clock calls, then later verify
    # that the they're within the segment's validity window (as_of and void_after)
    monotonic_before = monotonic_time()
    real = :os.system_time(:nanosecond)
    monotonic_after = monotonic_time()

    cond do
      segment_size != byte_size(binary) ->
        {:retry, :incorrect_segment_size}

      generation == 0 ->
        {:retry, :segment_not_initialized}

      rem(generation, 2) != 0 ->
        {:retry, :segment_undergoing_write}

      version != 2 ->
        {:error, :incorrect_segment_version}

      max_drift > 1_000_000_000 ->
        {:error, :max_drift_too_large}

      clock_status != 1 ->
        {:retry, :clock_not_synchronized}

      # the segment is expired (or not yet valid), let's try one more time before bailing out
      monotonic_before < as_of or monotonic_after > void_after ->
        {:retry, :segment_expired_or_not_yet_valid}

      true ->
        # increase the error bound by the maximum drift the clock may have experienced since the clockbound shm was updated
        #
        # max_drift is the number of nanoseconds the clock has drifted per second elapsed
        duration_sec = :erlang.convert_time_unit(monotonic_before - as_of, :nanosecond, :second)
        bound = bound + duration_sec * max_drift

        earliest = DateTime.from_unix!(real - bound, :nanosecond)
        latest = DateTime.from_unix!(real + bound, :nanosecond)

        {:ok, %GlobalTimestamp{earliest: earliest, latest: latest}}
    end
  end

  def load_nif do
    Application.app_dir(:craft, ["priv", "clock_monotonic"]) |> :erlang.load_nif(0)
  end
  def clock_monotonic, do: :erlang.nif_error(:nif_not_loaded)
  def clock_monotonic_coarse, do: :erlang.nif_error(:nif_not_loaded)

  def monotonic_time do
    timespec =
      case clock_monotonic_coarse() do
        :error ->
          clock_monotonic()

        timespec ->
          timespec
      end

    timespec_to_nanosecond(timespec)
  end

  defp timespec_to_nanosecond({sec, nsec}) do
    :erlang.convert_time_unit(sec, :second, :nanosecond) + nsec
  end
end
