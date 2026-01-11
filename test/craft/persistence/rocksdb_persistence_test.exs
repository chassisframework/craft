defmodule Craft.Persistence.RocksDBPersistenceTest do
  use ExUnit.Case, async: true

  alias Craft.Persistence.RocksDBPersistence
  alias Craft.Configuration
  alias Craft.Log.CommandEntry
  alias Craft.Log.SnapshotEntry

  setup do
    group_name = :crypto.strong_rand_bytes(8)

    Configuration.delete_member_data(group_name)
    Configuration.write_new!(group_name, %{})

    state =
      group_name
      |> RocksDBPersistence.new()
      |> RocksDBPersistence.put_metadata(%{__struct__: :nope, some: :metadata})

    on_exit(fn ->
      RocksDBPersistence.close(state)
    end)

    [state: state]
  end

  describe "append/2" do
    test "without an existing write buffer", %{state: state} do
      state =
        Enum.reduce(1..5, state, fn i, state ->
          RocksDBPersistence.append(state, %CommandEntry{command: i, term: i})
        end)

      assert %{
               log: [
                 {1, %CommandEntry{term: 1, command: 1}},
                 {2, %CommandEntry{term: 2, command: 2}},
                 {3, %CommandEntry{term: 3, command: 3}},
                 {4, %CommandEntry{term: 4, command: 4}},
                 {5, %CommandEntry{term: 5, command: 5}},
               ]
             } = RocksDBPersistence.dump(state)

      assert RocksDBPersistence.latest_index(state) == 5
      assert RocksDBPersistence.latest_term(state) == 5
    end

    test "flushes an existing write buffer", %{state: state} do
      assert {state, 1} = RocksDBPersistence.buffer_append(state, %CommandEntry{term: 1, command: :buffered})
      assert {state, 2} = RocksDBPersistence.buffer_append(state, %CommandEntry{term: 2, command: :buffered})

      state =
        Enum.reduce(3..5, state, fn i, state ->
          RocksDBPersistence.append(state, %CommandEntry{command: i, term: i})
        end)

      assert %{
               log: [
                 {1, %CommandEntry{term: 1, command: :buffered}},
                 {2, %CommandEntry{term: 2, command: :buffered}},
                 {3, %CommandEntry{term: 3, command: 3}},
                 {4, %CommandEntry{term: 4, command: 4}},
                 {5, %CommandEntry{term: 5, command: 5}},
               ]
             } = RocksDBPersistence.dump(state)

      assert is_nil(state.write_buffer)

      assert RocksDBPersistence.latest_index(state) == 5
      assert RocksDBPersistence.latest_term(state) == 5
    end
  end

  describe "rewind/2" do
    test "without an existing write buffer", %{state: state} do
      state =
        Enum.reduce(1..5, state, fn i, state ->
          RocksDBPersistence.append(state, %CommandEntry{command: i, term: i})
        end)

      state = RocksDBPersistence.rewind(state, 2)

      assert %{
               log: [
                 {1, %CommandEntry{term: 1, command: 1}},
                 {2, %CommandEntry{term: 2, command: 2}},
               ]
             } = RocksDBPersistence.dump(state)

      assert RocksDBPersistence.latest_index(state) == 2
      assert RocksDBPersistence.latest_term(state) == 2
    end

    test "flushes an existing write buffer", %{state: state} do
      state =
        Enum.reduce(1..5, state, fn i, state ->
          RocksDBPersistence.append(state, %CommandEntry{command: i, term: i})
        end)

      state =
        Enum.reduce(6..10, state, fn i, state ->
          assert {state, ^i} = RocksDBPersistence.buffer_append(state, %CommandEntry{command: i, term: :buffered})

          state
        end)

      state = RocksDBPersistence.rewind(state, 2)

      assert is_nil(state.write_buffer)

      assert %{
               log: [
                 {1, %CommandEntry{term: 1, command: 1}},
                 {2, %CommandEntry{term: 2, command: 2}},
               ]
             } = RocksDBPersistence.dump(state)

      assert RocksDBPersistence.latest_index(state) == 2
      assert RocksDBPersistence.latest_term(state) == 2
    end
  end

  test "fetch/2", %{state: state} do
    state =
      Enum.reduce(1..5, state, fn i, state ->
        RocksDBPersistence.append(state, %CommandEntry{command: i, term: i})
      end)
    
    assert {:ok, %CommandEntry{term: 3, command: 3}} = RocksDBPersistence.fetch(state, 3)
  end

  test "fetch_from/2", %{state: state} do
    state =
      Enum.reduce(1..5, state, fn i, state ->
        RocksDBPersistence.append(state, %CommandEntry{command: i, term: i})
      end)

    assert [
             %CommandEntry{term: 3, command: 3},
             %CommandEntry{term: 4, command: 4},
             %CommandEntry{term: 5, command: 5}
           ] = RocksDBPersistence.fetch_from(state, 3)
  end

  test "fetch_between/2", %{state: state} do
    state =
      Enum.reduce(1..7, state, fn i, state ->
        RocksDBPersistence.append(state, %CommandEntry{command: i, term: i})
      end)

    assert [
             %CommandEntry{term: 2, command: 2},
             %CommandEntry{term: 3, command: 3},
             %CommandEntry{term: 4, command: 4},
             %CommandEntry{term: 5, command: 5},
           ] = RocksDBPersistence.fetch_between(state, 2..5)
  end

  test "truncate/3", %{state: state} do
    state =
      Enum.reduce(1..5, state, fn i, state ->
        RocksDBPersistence.append(state, %CommandEntry{command: i, term: i})
      end)

    state = RocksDBPersistence.truncate(state, 3, %SnapshotEntry{term: 3})

    assert %{
             log: [
               {3, %SnapshotEntry{term: 3}},
               {4, %CommandEntry{term: 4, command: 4}},
               {5, %CommandEntry{term: 5, command: 5}},
             ]
           } = RocksDBPersistence.dump(state)
  end

  test "reverse_find/2", %{state: state} do
    state =
      Enum.reduce(1..5, state, fn i, state ->
        RocksDBPersistence.append(state, %CommandEntry{command: i, term: i})
      end)

    assert %CommandEntry{command: 2, term: 2} = RocksDBPersistence.reverse_find(state, fn entry -> entry.command == 2 end)
  end

  test "length/1", %{state: state} do
    state =
      Enum.reduce(1..5, state, fn i, state ->
        RocksDBPersistence.append(state, %CommandEntry{command: i, term: i})
      end)

    assert RocksDBPersistence.length(state) == 5
  end

  test "put_metadata/2", %{state: state} do
    state = RocksDBPersistence.append(state, %CommandEntry{command: 1, term: 1})
    state = RocksDBPersistence.put_metadata(state, %{__struct__: :abc, a: :b, c: :d})

    assert %{metadata: [{"metadata", %{a: :b, c: :d}}]} = RocksDBPersistence.dump(state)
  end

  test "fetch_metadata/1", %{state: state} do
    state = RocksDBPersistence.append(state, %CommandEntry{command: 1, term: 1})

    assert %{metadata: [{"metadata", %{some: :metadata}}]} = RocksDBPersistence.dump(state)
  end

  # TODO: random buffering operations would be a good model property test
  describe "buffering" do
    test "buffer_rewind also deletes uncommitted entries", %{state: state} do
      state =
        Enum.reduce(1..5, state, fn i, state ->
          RocksDBPersistence.append(state, %CommandEntry{command: i, term: i})
        end)

      state =
        Enum.reduce(6..10, state, fn i, state ->
          assert {state, ^i} = RocksDBPersistence.buffer_append(state, %CommandEntry{term: i, command: :buffered})

          state
        end)

      state = RocksDBPersistence.buffer_rewind(state, 3)

      assert {state, 4} = RocksDBPersistence.buffer_append(state, %CommandEntry{term: 4, command: :buffered})

      state = RocksDBPersistence.commit_buffer(state)

      assert %{
               log: [
                 {1, %CommandEntry{term: 1, command: 1}},
                 {2, %CommandEntry{term: 2, command: 2}},
                 {3, %CommandEntry{term: 3, command: 3}},
                 {4, %CommandEntry{term: 4, command: :buffered}},
               ]
             } = RocksDBPersistence.dump(state)

      assert RocksDBPersistence.latest_index(state) == 4
      assert RocksDBPersistence.latest_term(state) == 4
    end
  end
end
