defmodule Craft.Persistence.ETSPersistenceTest do
  use ExUnit.Case, async: true

  alias Craft.Persistence.ETSPersistence
  alias Craft.Persistence.Metadata
  alias Craft.Log.CommandEntry
  alias Craft.Log.SnapshotEntry

  setup do
    group_name = :crypto.strong_rand_bytes(8) |> Base.encode16() |> String.to_atom()

    state =
      group_name
      |> ETSPersistence.new()
      |> ETSPersistence.put_metadata(%{__struct__: :nope, some: :metadata})

    [state: state]
  end

  describe "append/2" do
    test "without an existing write buffer", %{state: state} do
      state =
        Enum.reduce(1..5, state, fn i, state ->
          ETSPersistence.append(state, %CommandEntry{command: i, term: i})
        end)

      assert %{
               log: [
                 {1, %CommandEntry{term: 1, command: 1}},
                 {2, %CommandEntry{term: 2, command: 2}},
                 {3, %CommandEntry{term: 3, command: 3}},
                 {4, %CommandEntry{term: 4, command: 4}},
                 {5, %CommandEntry{term: 5, command: 5}},
               ]
             } = ETSPersistence.dump(state)

      assert ETSPersistence.latest_index(state) == 5
      assert ETSPersistence.latest_term(state) == 5
    end

    test "flushes an existing write buffer", %{state: state} do
      assert {state, 1} = ETSPersistence.buffer_append(state, %CommandEntry{term: 1, command: :buffered})
      assert {state, 2} = ETSPersistence.buffer_append(state, %CommandEntry{term: 2, command: :buffered})

      state =
        Enum.reduce(3..5, state, fn i, state ->
          ETSPersistence.append(state, %CommandEntry{command: i, term: i})
        end)

      assert %{
               log: [
                 {1, %CommandEntry{term: 1, command: :buffered}},
                 {2, %CommandEntry{term: 2, command: :buffered}},
                 {3, %CommandEntry{term: 3, command: 3}},
                 {4, %CommandEntry{term: 4, command: 4}},
                 {5, %CommandEntry{term: 5, command: 5}},
               ]
             } = ETSPersistence.dump(state)

      assert :ets.info(state.write_buffer_table, :size) == 0

      assert ETSPersistence.latest_index(state) == 5
      assert ETSPersistence.latest_term(state) == 5
    end
  end

  describe "rewind/2" do
    test "when there's nothing to be rewound", %{state: state} do
      state = ETSPersistence.append(state, %CommandEntry{command: 1, term: 1})

      state = ETSPersistence.rewind(state, 1)

      assert %{
               log: [
                 {1, %CommandEntry{term: 1, command: 1}},
               ]
             } = ETSPersistence.dump(state)

      assert ETSPersistence.latest_index(state) == 1
      assert ETSPersistence.latest_term(state) == 1
    end

    test "without an existing write buffer", %{state: state} do
      state =
        Enum.reduce(1..5, state, fn i, state ->
          ETSPersistence.append(state, %CommandEntry{command: i, term: i})
        end)

      state = ETSPersistence.rewind(state, 2)

      assert %{
               log: [
                 {1, %CommandEntry{term: 1, command: 1}},
                 {2, %CommandEntry{term: 2, command: 2}},
               ]
             } = ETSPersistence.dump(state)

      assert ETSPersistence.latest_index(state) == 2
      assert ETSPersistence.latest_term(state) == 2
    end

    test "flushes an existing write buffer", %{state: state} do
      state =
        Enum.reduce(1..5, state, fn i, state ->
          ETSPersistence.append(state, %CommandEntry{command: i, term: i})
        end)

      state =
        Enum.reduce(6..10, state, fn i, state ->
          assert {state, ^i} = ETSPersistence.buffer_append(state, %CommandEntry{command: i, term: :buffered})

          state
        end)

      state = ETSPersistence.rewind(state, 2)

      assert :ets.info(state.write_buffer_table, :size) == 0

      assert %{
               log: [
                 {1, %CommandEntry{term: 1, command: 1}},
                 {2, %CommandEntry{term: 2, command: 2}},
               ]
             } = ETSPersistence.dump(state)

      assert ETSPersistence.latest_index(state) == 2
      assert ETSPersistence.latest_term(state) == 2
    end
  end

  test "fetch/2", %{state: state} do
    state =
      Enum.reduce(1..5, state, fn i, state ->
        ETSPersistence.append(state, %CommandEntry{command: i, term: i})
      end)
    
    assert {:ok, %CommandEntry{term: 3, command: 3}} = ETSPersistence.fetch(state, 3)
  end

  test "fetch_from/2", %{state: state} do
    state =
      Enum.reduce(1..5, state, fn i, state ->
        ETSPersistence.append(state, %CommandEntry{command: i, term: i})
      end)

    assert [
             %CommandEntry{term: 3, command: 3},
             %CommandEntry{term: 4, command: 4},
             %CommandEntry{term: 5, command: 5}
           ] = ETSPersistence.fetch_from(state, 3)
  end

  test "fetch_between/2", %{state: state} do
    state =
      Enum.reduce(1..7, state, fn i, state ->
        ETSPersistence.append(state, %CommandEntry{command: i, term: i})
      end)

    assert [
             %CommandEntry{term: 2, command: 2},
             %CommandEntry{term: 3, command: 3},
             %CommandEntry{term: 4, command: 4},
             %CommandEntry{term: 5, command: 5},
           ] = ETSPersistence.fetch_between(state, 2..5)
  end

  test "truncate/3", %{state: state} do
    state =
      Enum.reduce(1..5, state, fn i, state ->
        ETSPersistence.append(state, %CommandEntry{command: i, term: i})
      end)

    state = ETSPersistence.truncate(state, 3, %SnapshotEntry{term: 3})

    assert %{
             log: [
               {3, %SnapshotEntry{term: 3}},
               {4, %CommandEntry{term: 4, command: 4}},
               {5, %CommandEntry{term: 5, command: 5}},
             ]
           } = ETSPersistence.dump(state)
  end

  test "reverse_find/2", %{state: state} do
    state =
      Enum.reduce(1..5, state, fn i, state ->
        ETSPersistence.append(state, %CommandEntry{command: i, term: i})
      end)

    assert %CommandEntry{command: 2, term: 2} = ETSPersistence.reverse_find(state, fn entry -> entry.command == 2 end)
  end

  test "length/1", %{state: state} do
    state =
      Enum.reduce(1..5, state, fn i, state ->
        ETSPersistence.append(state, %CommandEntry{command: i, term: i})
      end)

    assert ETSPersistence.length(state) == 5
  end

  test "put_metadata/2 + fetch_metadata/1", %{state: state} do
    state = ETSPersistence.append(state, %CommandEntry{command: 1, term: 1})
    state = ETSPersistence.put_metadata(state, %{current_term: 1234})

    assert {:ok, %Metadata{current_term: 1234}} = ETSPersistence.fetch_metadata(state)
  end

  # # TODO: random buffering operations would be a good model property test
  describe "buffering" do
    test "buffer_rewind also deletes uncommitted entries", %{state: state} do
      state =
        Enum.reduce(1..5, state, fn i, state ->
          ETSPersistence.append(state, %CommandEntry{command: i, term: i})
        end)

      state =
        Enum.reduce(6..10, state, fn i, state ->
          assert {state, ^i} = ETSPersistence.buffer_append(state, %CommandEntry{term: i, command: :buffered})

          state
        end)

      state = ETSPersistence.buffer_rewind(state, 3)

      assert {state, 4} = ETSPersistence.buffer_append(state, %CommandEntry{term: 4, command: :buffered})

      state = ETSPersistence.commit_buffer(state)

      assert %{
               log: [
                 {1, %CommandEntry{term: 1, command: 1}},
                 {2, %CommandEntry{term: 2, command: 2}},
                 {3, %CommandEntry{term: 3, command: 3}},
                 {4, %CommandEntry{term: 4, command: :buffered}},
               ]
             } = ETSPersistence.dump(state)

      assert ETSPersistence.latest_index(state) == 4
      assert ETSPersistence.latest_term(state) == 4
    end
  end
end
