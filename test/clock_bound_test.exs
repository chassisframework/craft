defmodule ClockBoundClientTest do
  use ExUnit.Case

  setup do
    monotonic_time = ClockBound.Client.monotonic_time()
    {as_of_s, as_of_ns} = {div(monotonic_time, 1_000_000_000), rem(monotonic_time, 1_000_000_000)}
    void_after_s = as_of_s + 60
    void_after_ns = as_of_ns

    test_data = %{
      segment_size: 800,
      version: 3,
      generation: 10,
      as_of: {as_of_s, as_of_ns},
      void_after: {void_after_s, void_after_ns},
      bound: 1000,
      max_drift: 5,
      clock_status: :synchronized
    }

    # Build test binary buffer
    binary_data =
      <<
        # magic_0 'AMZN'
        0x414D5A4E::unsigned-native-32,
        # magic_1 'CB'
        0x43420200::unsigned-native-32,
        # segment_size
        test_data[:segment_size]::unsigned-native-32,
        # version
        test_data[:version]::unsigned-native-16,
        # generation
        test_data[:generation]::unsigned-native-16,
        # as_of_sec
        as_of_s::signed-native-64,
        # as_of_nsec
        as_of_ns::signed-native-64,
        # void_after_sec
        void_after_s::signed-native-64,
        # void_after_nsec
        void_after_ns::signed-native-64,
        # bound
        test_data[:bound]::signed-native-64,
        # max_drift
        test_data[:max_drift]::unsigned-native-32,
        # reserved
        0::unsigned-native-32,
        # clock_status (synchronized)
        1::signed-native-32
      >> <> :binary.copy(<<0>>, 800 - 80)

    test_path = Path.join(System.tmp_dir!(), "clockbound_test.shm")
    File.write!(test_path, binary_data)

    invalid_path = Path.join(System.tmp_dir!(), "invalid_clockbound_test.shm")
    File.write!(invalid_path, <<0>>)

    on_exit(fn ->
      File.rm(test_path)
      File.rm(invalid_path)
    end)

    {:ok, path: test_path, invalid_path: invalid_path, test_data: test_data}
  end

  test "ClockBound.Client.now/1 with test file", %{path: path, test_data: test_data} do
    assert {:ok, data} = ClockBound.Client.now(path)
    assert data.clock_status == test_data.clock_status
  end

  test "ClockBound.Client.now/1 with invalid data", %{invalid_path: invalid_path} do
    assert {:error, :invalid_clockbound_data} = ClockBound.Client.now(invalid_path)
  end

  test "ClockBound.Client.read/1 with test file", %{path: path, test_data: test_data} do
    assert {:ok, data} = ClockBound.Client.read(path)
    assert data.segment_size == test_data[:segment_size]
    assert data.version == test_data[:version]
    assert data.generation == test_data[:generation]
    {as_of_sec, as_of_ns} = test_data[:as_of]
    {void_after_sec, void_after_ns} = test_data[:void_after]
    assert data.as_of == {as_of_sec, as_of_ns}
    assert data.void_after == {void_after_sec, void_after_ns}
    assert data.bound == test_data[:bound]
    assert data.max_drift == test_data[:max_drift]
    assert data.clock_status == test_data[:clock_status]
  end

  test "ClockBound.Client.read/1 with non-existent file" do
    non_existent_path = Path.join(System.tmp_dir!(), "non_existent.shm")
    assert {:error, :enoent} = ClockBound.Client.read(non_existent_path)
  end

  test "ClockBound.Client.compare/2" do
    now = DateTime.utc_now()
    five_seconds_ago = DateTime.add(now, -5, :second)

    cb1 = %ClockBound.Client{
      earliest: DateTime.add(now, -5, :nanosecond),
      latest: DateTime.add(now, 5, :nanosecond),
      clock_status: :synchronized
    }

    cb2 = %ClockBound.Client{
      earliest: DateTime.add(five_seconds_ago, -5, :nanosecond),
      latest: DateTime.add(five_seconds_ago, 5, :nanosecond),
      clock_status: :synchronized
    }

    cb3 = %ClockBound.Client{
      earliest: DateTime.add(now, -1, :second),
      latest: now,
      clock_status: :synchronized
    }

    assert :gt = ClockBound.Client.compare(cb1, cb2)
    assert :lt = ClockBound.Client.compare(cb2, cb1)
    assert :ov = ClockBound.Client.compare(cb1, cb1)
    assert :ov = ClockBound.Client.compare(cb1, cb3)
  end
end
