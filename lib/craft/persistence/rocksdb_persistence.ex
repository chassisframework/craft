defmodule Craft.Persistence.RocksDBPersistence do
  @moduledoc """
  Notes:
  - rocksdb's default comparator is lexicographic. when given positive integer terms, :erlang.term_to_binary/1
    outputs lexicographically ascending keys, so we can use rocks' default iterator to walk log indexes
  """
  @behaviour Craft.Persistence

  alias Craft.Configuration

  require Logger

  import Craft.Tracing, only: [logger_metadata: 1]

  @log_column_family {~c"log", []}
  @metadata_column_family {~c"metadata", []}

  defmodule WriteBuffer do
    defstruct [:batch, :index]

    def new(state) do
      {:ok, batch} = :rocksdb.batch()

      %__MODULE__{batch: batch, index: state.latest_index}
    end
  end

  defstruct [
    :db,
    :log_cf,
    :metadata_cf,
    :earliest_index,
    :latest_index,
    :latest_term,
    :write_opts,
    :write_buffer
  ]

  @impl true
  def new(group_name, opts \\ []) do
    group_dir =
      group_name
      |> Configuration.find()
      |> Map.fetch!(:data_dir)

    data_dir =
      Configuration.data_dir()
      |> Path.join(group_dir)
      |> log_dir()

    File.mkdir_p!(data_dir)

    write_opts = Keyword.get(opts, :write_opts, [sync: true])

    db_opts = [create_if_missing: true, create_missing_column_families: true, compression: :none, total_threads: System.schedulers_online()]

    {:ok, db, [_default, log_column_family_handle, metadata_column_family_handle]} =
      data_dir
      |> :erlang.binary_to_list()
      |> :rocksdb.open(db_opts, [{~c"default", []}, @log_column_family, @metadata_column_family])

    %__MODULE__{
      db: db,
      log_cf: log_column_family_handle,
      metadata_cf: metadata_column_family_handle,
      write_opts: write_opts
    }
    |> cache_index_and_term_bounds()
  end

  defp log_dir(base), do: Path.join(base, "log")

  @impl true
  def latest_index(%__MODULE__{} = state), do: state.latest_index

  @impl true
  def latest_term(%__MODULE__{} = state), do: state.latest_term

  @impl true
  def fetch(%__MODULE__{} = state, index) do
    case :rocksdb.get(state.db, state.log_cf, encode(index), []) do
      {:ok, entry} ->
        {:ok, decode(entry)}

      :not_found ->
        :error
    end
  end

  @impl true
  def fetch_from(%__MODULE__{} = state, index) do
    fetch_between(state, index..state.latest_index//1)
  end

  @impl true
  def fetch_between(%__MODULE__{} = state, index_range) do
    keys = Enum.map(index_range, &encode/1)

    :rocksdb.multi_get(state.db, state.log_cf, keys, [])
    |> Enum.map(fn {:ok, entry} -> decode(entry) end)
  end

  @impl true
  def buffer_append(%__MODULE__{} = state, %_struct{} = entry) do
    write_buffer = state.write_buffer || WriteBuffer.new(state)
    state = %{state | write_buffer: write_buffer}

    index = write_buffer.index + 1
    :ok = :rocksdb.batch_put(write_buffer.batch, state.log_cf, encode(index), encode(entry))

    Logger.debug("appended log entry to batch", logger_metadata(trace: {:appended, [entry]}))

    {put_in(state.write_buffer.index, index), index}
  end

  @impl true
  def buffer_rewind(%__MODULE__{} = state, index) do
    write_buffer = state.write_buffer || WriteBuffer.new(state)
    state = %{state | write_buffer: write_buffer}

    end_index = max(state.latest_index, write_buffer.index)

    Enum.each(index+1..end_index//1, fn index ->
      :ok = :rocksdb.batch_delete(write_buffer.batch, state.log_cf, encode(index))
    end)

    put_in(state.write_buffer.index, index)
  end

  @impl true
  def buffer_metadata_put(%__MODULE__{} = state, metadata) do
    write_buffer = state.write_buffer || WriteBuffer.new(state)
    state = %{state | write_buffer: write_buffer}

    :ok = :rocksdb.batch_put(write_buffer.batch, state.metadata_cf, "metadata", encode(metadata))

    state
  end

  @impl true
  def commit_buffer(%__MODULE__{} = state) do
    if state.write_buffer do
      :ok = :rocksdb.write_batch(state.db, state.write_buffer.batch, state.write_opts)

      # TODO: better log
      Logger.debug("wrote batch log entries", logger_metadata(trace: :wrote_batch))

      state
      |> release_buffer()
      |> cache_index_and_term_bounds()
    else
      state
    end
  end

  @impl true
  def release_buffer(%__MODULE__{write_buffer: nil} = state), do: state
  def release_buffer(%__MODULE__{} = state) do
    :ok = :rocksdb.release_batch(state.write_buffer.batch)

    %{state | write_buffer: nil}
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
    index = encode(index)

    {:ok, batch} = :rocksdb.batch()
    :ok = :rocksdb.batch_delete_range(batch, state.log_cf, encode(state.earliest_index), index)
    :ok = :rocksdb.batch_put(batch, state.log_cf, index, encode(snapshot_entry))
    :ok = :rocksdb.write_batch(state.db, batch, state.write_opts)
    :ok = :rocksdb.release_batch(batch)

    cache_index_and_term_bounds(state)
  end

  @impl true
  def reverse_find(%__MODULE__{} = state, fun) do
    {:ok, iterator} = :rocksdb.iterator(state.db, state.log_cf, [])
    do_reverse_find(iterator, state.latest_index, fun)
  end

  defp do_reverse_find(iterator, index, fun) do
    case :rocksdb.iterator_move(iterator, encode(index)) do
      {:ok, _index, value} ->
        entry = decode(value)

        if fun.(entry) do
          :ok = :rocksdb.iterator_close(iterator)
          entry
        else
          do_reverse_find(iterator, index - 1, fun)
        end

      _ ->
        :ok = :rocksdb.iterator_close(iterator)
        nil
    end
  end

  @impl true
  def reduce_while(%__MODULE__{} = state, acc, fun) do
    {:ok, iterator} = :rocksdb.iterator(state.db, state.log_cf, [])
    with {:ok, index, value} <- :rocksdb.iterator_move(iterator, :first),
         {:cont, acc} <- fun.({decode(index), decode(value)}, acc) do
        Stream.repeatedly(fn ->
          case :rocksdb.iterator_move(iterator, :next) do
            {:ok, index, value} ->
              {decode(index), decode(value)}

            _ ->
              :ok = :rocksdb.iterator_close(iterator)
              :eof
          end
        end)
        |> Stream.take_while(fn
          :eof ->
            false

          _ ->
            true
        end)
        |> Enum.reduce_while(acc, fun)
    else
      {:halt, acc} ->
        acc

      {:error, :invalid_iterator} ->
        acc
    end
  end

  @impl true
  def length(%__MODULE__{} = state) do
    state.latest_index - state.earliest_index + 1
  end

  @impl true
  def put_metadata(%__MODULE__{} = state, metadata) do
    state
    |> buffer_metadata_put(metadata)
    |> commit_buffer()
  end

  @impl true
  def fetch_metadata(%__MODULE__{} = state) do
    case :rocksdb.get(state.db, state.metadata_cf, "metadata", []) do
      {:ok, binary} ->
        {:ok, struct(Craft.Persistence.Metadata, decode(binary))}

      _ ->
        :error
    end
  end

  @impl true
  def backup(%__MODULE__{} = state, to_directory) do
    dir =
      to_directory
      |> log_dir()
      |> :erlang.binary_to_list()

    :rocksdb.checkpoint(state.db, dir)
  end

  @impl true
  def close(%__MODULE__{} = state) do
    :rocksdb.close(state.db)
  end

  @impl true
  def dump(%__MODULE__{} = state) do
    %{
      log: do_dump(state.db, state.log_cf),
      metadata: do_dump(state.db, state.metadata_cf)
    }
  end

  defp do_dump(db, cf) do
    {:ok, iterator} = :rocksdb.iterator(db, cf, [])
    case :rocksdb.iterator_move(iterator, :first) do
      {:error, :invalid_iterator} ->
        :empty

      {:ok, index, value} ->
        Stream.repeatedly(fn ->
          case :rocksdb.iterator_move(iterator, :next) do
            {:ok, index, value} ->
              {index, value}

            _ ->
              :ok = :rocksdb.iterator_close(iterator)
              :eof
          end
        end)
        |> Stream.take_while(fn
          :eof ->
            false

          _ ->
            true
        end)
        |> Enum.concat([{index, value}])
        |> Enum.map(fn {k, v} ->
          try do
            {decode(k), decode(v)}
          rescue
            _ ->
              {k, decode(v)}
          end
        end)
        |> Enum.sort()
    end
  end

  defp encode(term), do: :erlang.term_to_binary(term)
  defp decode(binary), do: :erlang.binary_to_term(binary)

  defp cache_index_and_term_bounds(%__MODULE__{} = state) do
    {earliest_index, latest_index, latest_term} =
      with {:ok, iterator} <- :rocksdb.iterator(state.db, state.log_cf, []),
           {:ok, earliest_index, _} <- :rocksdb.iterator_move(iterator, :first),
           {:ok, latest_index, entry} <- :rocksdb.iterator_move(iterator, :last) do
        :ok = :rocksdb.iterator_close(iterator)
        {decode(earliest_index), decode(latest_index), decode(entry).term}
      else
        {:error, :invalid_iterator} ->
          {0, 0, -1}
      end

    %{state | earliest_index: earliest_index, latest_index: latest_index, latest_term: latest_term}
  end
end
