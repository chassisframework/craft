defmodule Craft.Persistence.ETSPersistence do
  @moduledoc """
  This module should not be used in production, it does not write to disk.
  """
  @behaviour Craft.Persistence

  require Logger

  import Craft.Tracing, only: [logger_metadata: 1]

  # write buffer stores an ordered list of operations
  defstruct [
    :table,
    :write_buffer_table,
    :buffered_metadata,
    :metadata
  ]

  # TODO: experiment with :read_concurrency
  @impl true
  def new(group_name, _opts \\ []) do
    table_name = Module.concat(__MODULE__, group_name)
    buffer_table_name = Module.concat(table_name, :write_buffer)

    %__MODULE__{
      table: :ets.new(table_name, [:ordered_set]),
      write_buffer_table: :ets.new(buffer_table_name, [:ordered_set, :private])
    }
    |> release_buffer()
  end

  @impl true
  def latest_index(%__MODULE__{} = state) do
    case :ets.last(state.table) do
      :"$end_of_table" ->
        0

      index ->
        index
    end
  end

  @impl true
  def latest_term(%__MODULE__{} = state) do
    case :ets.last_lookup(state.table) do
      {index, [{index, %{term: term}}]} ->
        term

      :"$end_of_table" ->
        -1
    end
  end

  @impl true
  def fetch(%__MODULE__{} = state, index) do
    case :ets.lookup(state.table, index) do
      [{_index, entry}] ->
        {:ok, entry}

      [] ->
        :error
    end
  end

  @impl true
  def fetch_from(%__MODULE__{} = state, index) do
    fetch_between(state, index..latest_index(state)//1)
  end

  @impl true
  def fetch_between(%__MODULE__{} = state, index_range) do
    Enum.map(index_range, fn i -> {:ok, entry} = fetch(state, i); entry end)
  end

  @impl true
  def any_buffered_log_writes?(%__MODULE__{} = state) do
    :ets.last(state.write_buffer_table) != :"$end_of_table"
  end

  @impl true
  def buffer_append(%__MODULE__{} = state, %_struct{} = entry) do
    {seq, index} =
      case :ets.last_lookup(state.write_buffer_table) do
        {seq, [{seq, {:append, index, _entry}}]} ->
          {seq, index}

        {seq, [{seq, {:rewind, index}}]} ->
          {seq, index}

        :"$end_of_table" ->
          {0, latest_index(state)}
      end

    index = index + 1

    operation = {seq + 1, {:append, index, entry}}
    :ets.insert(state.write_buffer_table, operation)

    Logger.debug("appended log entry to ETS batch", logger_metadata(trace: operation))

    {state, index}
  end

  @impl true
  def buffer_rewind(%__MODULE__{} = state, index) do
    seq =
      case :ets.last(state.write_buffer_table) do
        :"$end_of_table" ->
          0

        seq ->
          seq
      end

    operation = {seq + 1, {:rewind, index}}
    :ets.insert(state.write_buffer_table, operation)

    Logger.debug("appended rewind to ETS batch", logger_metadata(trace: operation))

    state
  end

  @impl true
  def buffer_metadata_put(%__MODULE__{} = state, metadata) do
    %{state | buffered_metadata: metadata}
  end

  @impl true
  def commit_buffer(%__MODULE__{} = state) do
    case :ets.first(state.write_buffer_table) do
      :"$end_of_table" ->
        :ok

      seq ->
        do_commit_buffer(state, seq)
    end

    if state.buffered_metadata do
      %{state | metadata: state.buffered_metadata}
    else
      state
    end
    |> release_buffer()
  end

  defp do_commit_buffer(state, seq) do
    case :ets.lookup(state.write_buffer_table, seq) do
      [{seq, {:append, index, entry}}] ->
        :ets.insert(state.table, {index, entry})
        do_commit_buffer(state, seq + 1)

      [{seq, {:rewind, index}}] ->
        :ets.select_delete(state.table, [{{:"$1", :_}, [{:">", :"$1", index}], [true]}])
        do_commit_buffer(state, seq + 1)

      [] ->
        :ok
    end
  end

  @impl true
  def release_buffer(%__MODULE__{} = state) do
    :ets.delete_all_objects(state.write_buffer_table)

    %{state | buffered_metadata: nil}
  end

  @impl true
  def append(%__MODULE__{} = state, %_struct{} = entry) do
    {state, _index} = buffer_append(state, entry)

    commit_buffer(state)
  end

  @impl true
  def rewind(%__MODULE__{} = state, index) do
    state
    |> buffer_rewind(index)
    |> commit_buffer()
  end

  @impl true
  def truncate(%__MODULE__{} = state, index, snapshot_entry) do
    :ets.select_delete(state.table, [{{:"$1", :"$2"}, [{:"=<", :"$1", index}], [true]}])
    :ets.insert(state.table, {index, snapshot_entry})

    state
  end

  @impl true
  def reverse_find(%__MODULE__{} = state, fun) do
    case :ets.last(state.table) do
      :"$end_of_table" ->
        nil

      index ->
        do_reverse_find(state.table, index, fun)
    end
  end

  defp do_reverse_find(table, index, fun) do
    case :ets.lookup(table, index) do
      [{_index, entry}] ->
        if fun.(entry) do
          entry
        else
          do_reverse_find(table, index - 1, fun)
        end

      [] ->
        nil
    end
  end

  @impl true
  def reduce_while(%__MODULE__{} = state, acc, fun) do
    case :ets.first(state.table) do
      :"$end_of_table" ->
        acc

      index ->
        do_reduce_while(state.table, index, acc, fun)
    end
  end

  def do_reduce_while(table, index, acc, fun) do
    with [{index, entry}] <- :ets.lookup(table, index),
         {:cont, acc} <- fun.({index, entry}, acc) do
      do_reduce_while(table, index + 1, acc, fun)
    else
      {:halt, acc} ->
        acc

      [] ->
        acc
    end
  end

  @impl true
  def length(%__MODULE__{} = state) do
    :ets.info(state.table, :size)
  end

  @impl true
  def put_metadata(%__MODULE__{} = state, metadata) do
    state
    |> buffer_metadata_put(metadata)
    |> commit_buffer()
  end

  @impl true
  def fetch_metadata(%__MODULE__{metadata: nil}), do: :error
  def fetch_metadata(%__MODULE__{} = state) do
    {:ok, struct(Craft.Persistence.Metadata, state.metadata)}
  end

  @impl true
  def backup(_state, _to_directory) do
    :ok
  end

  @impl true
  def close(state) do
    :ets.delete(state.table)
    :ets.delete(state.write_buffer_table)

    :ok
  end

  @impl true
  def dump(%__MODULE__{} = state) do
    %{
      log: do_dump(state.table),
      write_buffer: do_dump(state.write_buffer_table),
      buffered_metadata: state.buffered_metadata,
      metadata: state.metadata
    }
  end

  defp do_dump(table) do
    :ets.foldr(fn obj, objs -> [obj | objs] end, [], table)
  end
end
