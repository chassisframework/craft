defmodule Craft.Persistence.MapPersistenceTest do
  use ExUnit.Case, async: true

  alias Craft.Persistence.MapPersistence
  alias Craft.Persistence.Metadata
  alias Craft.Log.CommandEntry
  alias Craft.Log.SnapshotEntry

  setup do
    state =
      MapPersistence.new(nil)
      |> MapPersistence.put_metadata(%{__struct__: :nope, some: :metadata})

    [state: state]
  end

  describe "append/2" do
    test "without an existing write buffer", %{state: state} do
      state =
        Enum.reduce(1..5, state, fn i, state ->
          MapPersistence.append(state, %CommandEntry{command: i, term: i})
        end)

      assert %{
               log: [
                 {1, %CommandEntry{term: 1, command: 1}},
                 {2, %CommandEntry{term: 2, command: 2}},
                 {3, %CommandEntry{term: 3, command: 3}},
                 {4, %CommandEntry{term: 4, command: 4}},
                 {5, %CommandEntry{term: 5, command: 5}},
               ]
             } = MapPersistence.dump(state)

      assert MapPersistence.latest_index(state) == 5
      assert MapPersistence.latest_term(state) == 5
    end

    test "flushes an existing write buffer", %{state: state} do
      assert {state, 1} = MapPersistence.buffer_append(state, %CommandEntry{term: 1, command: :buffered})
      assert {state, 2} = MapPersistence.buffer_append(state, %CommandEntry{term: 2, command: :buffered})

      state =
        Enum.reduce(3..5, state, fn i, state ->
          MapPersistence.append(state, %CommandEntry{command: i, term: i})
        end)

      assert %{
               log: [
                 {1, %CommandEntry{term: 1, command: :buffered}},
                 {2, %CommandEntry{term: 2, command: :buffered}},
                 {3, %CommandEntry{term: 3, command: 3}},
                 {4, %CommandEntry{term: 4, command: 4}},
                 {5, %CommandEntry{term: 5, command: 5}},
               ]
             } = MapPersistence.dump(state)

      assert state.write_buffer == []

      assert MapPersistence.latest_index(state) == 5
      assert MapPersistence.latest_term(state) == 5
    end
  end

  describe "rewind/2" do
    test "when there's nothing to be rewound", %{state: state} do
      state = MapPersistence.append(state, %CommandEntry{command: 1, term: 1})

      state = MapPersistence.rewind(state, 1)

      assert %{
               log: [
                 {1, %CommandEntry{term: 1, command: 1}},
               ]
             } = MapPersistence.dump(state)

      assert MapPersistence.latest_index(state) == 1
      assert MapPersistence.latest_term(state) == 1
    end

    test "without an existing write buffer", %{state: state} do
      state =
        Enum.reduce(1..5, state, fn i, state ->
          MapPersistence.append(state, %CommandEntry{command: i, term: i})
        end)

      state = MapPersistence.rewind(state, 2)

      assert %{
               log: [
                 {1, %CommandEntry{term: 1, command: 1}},
                 {2, %CommandEntry{term: 2, command: 2}},
               ]
             } = MapPersistence.dump(state)

      assert MapPersistence.latest_index(state) == 2
      assert MapPersistence.latest_term(state) == 2
    end

    test "flushes an existing write buffer", %{state: state} do
      state =
        Enum.reduce(1..5, state, fn i, state ->
          MapPersistence.append(state, %CommandEntry{command: i, term: i})
        end)

      state =
        Enum.reduce(6..10, state, fn i, state ->
          assert {state, ^i} = MapPersistence.buffer_append(state, %CommandEntry{command: i, term: :buffered})

          state
        end)

      state = MapPersistence.rewind(state, 2)

      assert state.write_buffer == []

      assert %{
               log: [
                 {1, %CommandEntry{term: 1, command: 1}},
                 {2, %CommandEntry{term: 2, command: 2}},
               ]
             } = MapPersistence.dump(state)

      assert MapPersistence.latest_index(state) == 2
      assert MapPersistence.latest_term(state) == 2
    end
  end

  test "fetch/2", %{state: state} do
    state =
      Enum.reduce(1..5, state, fn i, state ->
        MapPersistence.append(state, %CommandEntry{command: i, term: i})
      end)
    
    assert {:ok, %CommandEntry{term: 3, command: 3}} = MapPersistence.fetch(state, 3)
  end

  test "fetch_from/2", %{state: state} do
    state =
      Enum.reduce(1..5, state, fn i, state ->
        MapPersistence.append(state, %CommandEntry{command: i, term: i})
      end)

    assert [
             %CommandEntry{term: 3, command: 3},
             %CommandEntry{term: 4, command: 4},
             %CommandEntry{term: 5, command: 5}
           ] = MapPersistence.fetch_from(state, 3)
  end

  test "fetch_between/2", %{state: state} do
    state =
      Enum.reduce(1..7, state, fn i, state ->
        MapPersistence.append(state, %CommandEntry{command: i, term: i})
      end)

    assert [
             %CommandEntry{term: 2, command: 2},
             %CommandEntry{term: 3, command: 3},
             %CommandEntry{term: 4, command: 4},
             %CommandEntry{term: 5, command: 5},
           ] = MapPersistence.fetch_between(state, 2..5)
  end

  test "truncate/3", %{state: state} do
    state =
      Enum.reduce(1..5, state, fn i, state ->
        MapPersistence.append(state, %CommandEntry{command: i, term: i})
      end)

    state = MapPersistence.truncate(state, 3, %SnapshotEntry{term: 3})

    assert %{
             log: [
               {3, %SnapshotEntry{term: 3}},
               {4, %CommandEntry{term: 4, command: 4}},
               {5, %CommandEntry{term: 5, command: 5}},
             ]
           } = MapPersistence.dump(state)
  end

  test "reverse_find/2", %{state: state} do
    state =
      Enum.reduce(1..5, state, fn i, state ->
        MapPersistence.append(state, %CommandEntry{command: i, term: i})
      end)

    assert %CommandEntry{command: 2, term: 2} = MapPersistence.reverse_find(state, fn entry -> entry.command == 2 end)
  end

  test "length/1", %{state: state} do
    state =
      Enum.reduce(1..5, state, fn i, state ->
        MapPersistence.append(state, %CommandEntry{command: i, term: i})
      end)

    assert MapPersistence.length(state) == 5
  end

  test "put_metadata/2 + fetch_metadata/1", %{state: state} do
    state = MapPersistence.append(state, %CommandEntry{command: 1, term: 1})
    state = MapPersistence.put_metadata(state, %{current_term: 1234})

    assert {:ok, %Metadata{current_term: 1234}} = MapPersistence.fetch_metadata(state)
  end

  # # TODO: random buffering operations would be a good model property test
  describe "buffering" do
    test "buffer_rewind also deletes uncommitted entries", %{state: state} do
      state =
        Enum.reduce(1..5, state, fn i, state ->
          MapPersistence.append(state, %CommandEntry{command: i, term: i})
        end)

      state =
        Enum.reduce(6..10, state, fn i, state ->
          assert {state, ^i} = MapPersistence.buffer_append(state, %CommandEntry{term: i, command: :buffered})

          state
        end)

      state = MapPersistence.buffer_rewind(state, 3)

      assert {state, 4} = MapPersistence.buffer_append(state, %CommandEntry{term: 4, command: :buffered})

      state = MapPersistence.commit_buffer(state)

      assert %{
               log: [
                 {1, %CommandEntry{term: 1, command: 1}},
                 {2, %CommandEntry{term: 2, command: 2}},
                 {3, %CommandEntry{term: 3, command: 3}},
                 {4, %CommandEntry{term: 4, command: :buffered}},
               ]
             } = MapPersistence.dump(state)

      assert MapPersistence.latest_index(state) == 4
      assert MapPersistence.latest_term(state) == 4
    end
  end
end
