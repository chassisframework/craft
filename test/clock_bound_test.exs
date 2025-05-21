defmodule ClockBoundClientTest do
  use ExUnit.Case
  use PropCheck

  import Mox

  alias Craft.ClockBound.Client
  alias Craft.ClockBound.ClockMock

  setup :verify_on_exit!

  setup do
    # expected data from the test file
    test_shm_path = "test/fixtures/clockbound_test_shm"
    expected_data = %{
      version: 1,
      bound: 15023032,
      clock_status: :synchronized,
      max_drift: 50000,
      void_after: 2391606000000000,
      as_of: 2390606012758000,
      generation: 18546,
      segment_size: 72
    }

    invalid_file = Path.join(System.tmp_dir!(), "invalid_clockbound_shm")
    File.write!(invalid_file, <<0>>)

    on_exit(fn ->
      File.rm(invalid_file)
    end)

    {:ok, test_shm_path: test_shm_path, invalid_file: invalid_file, expected_data: expected_data}
  end

  test "Craft.ClockBound.Client.now/1 with test file", %{test_shm_path: test_shm_path, expected_data: expected_data} do
    property = forall monotonic_time <- integer() do
      ClockMock
      |> stub(:monotonic_time, fn -> monotonic_time end)

      if monotonic_time < expected_data[:as_of] or monotonic_time > expected_data[:void_after] do
        {:error, :causality_breach} == Client.now(test_shm_path)
      else
        assert {:ok, %Client{} = data} = Client.now(test_shm_path)
        data.clock_status == :synchronized
      end
    end

    PropCheck.quickcheck(property)
  end

  test "Craft.ClockBound.Client.now/1 with invalid data", %{invalid_file: invalid_file} do
    assert {:error, :invalid_clockbound_data} = Client.now(invalid_file)
  end

  test "Craft.ClockBound.Client.read/1 with test file", %{test_shm_path: test_shm_path, expected_data: expected_data} do
    assert {:ok, data} = Client.read(test_shm_path)
    assert data.segment_size == expected_data[:segment_size]
    assert data.version == expected_data[:version]
    assert data.generation == expected_data[:generation]
    assert data.as_of == expected_data[:as_of]
    assert data.void_after == expected_data[:void_after]
    assert data.bound == expected_data[:bound]
    assert data.max_drift == expected_data[:max_drift]
    assert data.clock_status == expected_data[:clock_status]
  end

  test "Craft.ClockBound.Client.read/1 with non-existent file" do
    non_existent_path = Path.join(System.tmp_dir!(), "non_existent.shm")
    assert {:error, :enoent} = Client.read(non_existent_path)
  end

  test "Craft.ClockBound.Client.compare/2" do
    now = DateTime.utc_now()
    five_seconds_ago = DateTime.add(now, -5, :second)

    cb1 = %Client{
      earliest: DateTime.add(now, -5, :nanosecond),
      latest: DateTime.add(now, 5, :nanosecond),
      clock_status: :synchronized
    }

    cb2 = %Client{
      earliest: DateTime.add(five_seconds_ago, -5, :nanosecond),
      latest: DateTime.add(five_seconds_ago, 5, :nanosecond),
      clock_status: :synchronized
    }

    cb3 = %Client{
      earliest: DateTime.add(now, -1, :second),
      latest: now,
      clock_status: :synchronized
    }

    assert :gt = Client.compare(cb1, cb2)
    assert :lt = Client.compare(cb2, cb1)
    assert :ov = Client.compare(cb1, cb1)
    assert :ov = Client.compare(cb1, cb3)
  end
end
