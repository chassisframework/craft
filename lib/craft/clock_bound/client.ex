defmodule Craft.ClockBound.Client do
  @moduledoc """
  This module provides functionality to retrieve clock error bounds and clock status from shared memory.

  ## Features

  - Reads binary data from a shared memory file and parses it according to the ClockBound protocol specification.
  - Provides the current clock error bounds (`earliest` and `latest`) and clock status.
  - Converts timestamps to `DateTime` for easier comparision.

  ## Usage

  1. Call `ClockBound.Client.now/0` or `ClockBound.Client.now/1` to retrieve the current clock error bounds and status:
    ```elixir
    case ClockBound.Client.now() do
      {:ok, clock_data} ->
        IO.inspect(clock_data)

      {:error, reason} ->
        _
    end
    ```
  2. Call `ClockBound.Client.compare/2` to compare two ClockBound windows:
    ```elixir
    case ClockBound.Client.compare(clock_data_1, clock_data_2) do
      :lt -> IO.puts("Clock 1 is less than Clock 2")
      :gt -> IO.puts("Clock 1 is greater than Clock 2")
      :ov -> IO.puts("Clock 1 and Clock 2 overlap")
    end
    ```

  ## Notes

  - The default shared memory path is `/var/run/clockbound/shm`. You can override this by updating the config value for `[:craft, :clockbound, :shm_path].
  - The module adheres to the ClockBound protocol specification, which can be found [here](https://github.com/aws/clock-bound/blob/main/docs/PROTOCOL.md).
  """

  alias Craft.ClockBound.Client
  alias Craft.ClockBound.Native

  @default_shm_path Application.compile_env(
                      :craft,
                      [:clock_bound, :shm_path],
                      "/var/run/clockbound/shm"
                    )

  defstruct [
    :earliest,
    :latest,
    :clock_status
  ]

  @type clock_status :: :unknown | :synchronized | :free_running | :invalid
  @type t :: %__MODULE__{
          earliest: DateTime.t(),
          latest: DateTime.t(),
          clock_status: clock_status()
        }

  @doc """
  Returns the current clock time with error bounds and clock status.
  """
  def now(shm_path \\ @default_shm_path) do
    with {:ok, data} <- read(shm_path),
         real = :os.system_time(:nanosecond),
         monotonic = monotonic_time(),
         {:ok, {earliest, latest, clock_status}} <- compute_bound_at(real, monotonic, data),
         {:ok, earliest} <- DateTime.from_unix(earliest, :nanosecond),
         {:ok, latest} <- DateTime.from_unix(latest, :nanosecond) do
      {:ok,
       %__MODULE__{
         earliest: earliest,
         latest: latest,
         clock_status: clock_status
       }}
    end
  end

  @doc """
  Compares two ClockBound windows and returns:

    * `:lt` if a is strictly younger than b
    * `:gt` if a is strictly older than b
    * `:ov` if the ranges overlap and no strict ordering can be determined
  """
  def compare(%Client{} = cbe_1, %Client{} = cbe_2) do
    cond do
      DateTime.after?(cbe_1.earliest, cbe_2.latest) -> :gt
      DateTime.before?(cbe_1.latest, cbe_2.earliest) -> :lt
      true -> :ov
    end
  end

  @doc """
  Returns the current monotonic time in nanoseconds
  For macOS, it uses the native NIF to get the monotonic time.
  For other platforms, it uses the Erlang system_info function.
  """
  def monotonic_time do
    os_monotonic_time = :erlang.system_info(:os_monotonic_time_source)

    if os_monotonic_time[:clock_id] == :monotonic do
      os_monotonic_time[:time]
    else
      timespec_to_nanosecond(Native.get_monotonic_time())
    end
  end

  # If max_drift is too large, it indicates that some issue in the clockbound data
  defp compute_bound_at(_real, _monotonic, %{max_drift: max_drift})
       when max_drift > 1_000_000_000 do
    {:error, :max_drift_too_large}
  end

  defp compute_bound_at(real, monotonic, clockbound_data) do
    %{
      as_of: as_of,
      void_after: void_after,
      clock_status: status,
      max_drift: max_drift,
      bound: bound
    } = clockbound_data

    # Validate whether the clockbound data read from shared memory is still valid at the time of the request by
    # checking if the current monotonic time is lesser than the void_after time.
    clock_status =
      if status in [:synchronized, :free_running] and monotonic < void_after,
        do: status,
        else: :unknown

    # Calculate the duration that has elapsed between the instant when the clockbound data were
    # written to the shared memory segment (approximated by `as_of`), and the instant when the
    # request to calculate the ClockErrorBound was actually requested (approximated by `monotonic`). This
    # duration is used to compute the growth of the error bound due to local dispersion
    # between polling chrony and now.
    #
    # Ideally, the current monotonic time should be greater than the time the clockbound data was written(as_of),
    # but sometimes monotonic time is observed to be older by a handful of nanoseconds. To avoid this
    # issue, we will introduce a small epsilon value (1ms) to account for clock precision.
    # The cause of this behavior is not clear
    causality_blur = as_of - 1000

    duration =
      cond do
        # Happy path, clockbound data is older than the current monotonic time.
        monotonic >= as_of -> monotonic - as_of
        # Causality is "almost" broken but still within the epsilon range of clock precision as mentioned above.
        # assume the monotonic time and the clockbound data updated time to be the same for this case (elapsed time = 0)
        monotonic > causality_blur -> 0
        # Causality is breached.
        true -> :causality_breach
      end

    if duration == :causality_breach do
      {:error, :causality_breach}
    else
      # Increase the bound on clock error with the maximum drift the clock may be experiencing
      # between the time the clockbound data was written and ~now.
      duration_sec = div(duration, 1_000_000_000)
      updated_bound = bound + duration_sec * max_drift
      # Build the (earliest, latest) interval within which true time exists.
      earliest = real - updated_bound
      latest = real + updated_bound

      {:ok, {earliest, latest, clock_status}}
    end
  end

  # Reads the clockbound data from the shared memory segment
  def read(shm_path \\ @default_shm_path) do
    case File.read(shm_path) do
      {:ok, bin} -> parse(bin)
      {:error, reason} -> {:error, reason}
    end
  end

  defp parse(binary_data) do
    case binary_data do
      <<0x414D5A4E::unsigned-native-32, 0x43420200::unsigned-native-32,
        segment_size::unsigned-native-32, version::unsigned-native-16,
        generation::unsigned-native-16, as_of_sec::signed-native-64, as_of_nsec::signed-native-64,
        void_after_sec::signed-native-64, void_after_nsec::signed-native-64,
        bound::signed-native-64, max_drift::unsigned-native-32, _reserved::unsigned-native-32,
        clock_status::signed-native-32, _padding::binary>> ->
        {:ok,
         %{
           segment_size: segment_size,
           version: version,
           generation: generation,
           as_of: timespec_to_nanosecond({as_of_sec, as_of_nsec}),
           void_after: timespec_to_nanosecond({void_after_sec, void_after_nsec}),
           bound: bound,
           max_drift: max_drift,
           clock_status: decode_clock_status(clock_status)
         }}

      _ ->
        {:error, :invalid_clockbound_data}
    end
  end

  def timespec_to_nanosecond({sec, nsec}) when is_integer(sec) and is_integer(nsec) do
    :erlang.convert_time_unit(sec, :second, :nanosecond) + nsec
  end

  defp decode_clock_status(0), do: :unknown
  defp decode_clock_status(1), do: :synchronized
  defp decode_clock_status(2), do: :free_running
  defp decode_clock_status(_), do: :invalid
end
