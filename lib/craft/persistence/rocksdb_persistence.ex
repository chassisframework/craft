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

  defstruct [
    :db,
    :log_cf,
    :metadata_cf,
    :latest_index,
    :latest_term,
    :write_opts
  ]

  @impl true
  def new(group_name, opts \\ []) do
    group_dir =
      group_name
      |> Configuration.find()
      |> Map.fetch!(:data_dir)

    data_dir = Path.join([Configuration.data_dir(), group_dir, "log"])

    File.mkdir_p!(data_dir)

    write_opts = Keyword.get(opts, :write_opts, [sync: true])

    db_opts = [create_if_missing: true, create_missing_column_families: true]

    {:ok, db, [_default, log_column_family_handle, metadata_column_family_handle]} =
      data_dir
      |> :erlang.binary_to_list()
      |> :rocksdb.open_optimistic_transaction_db(db_opts, [{~c"default", []}, @log_column_family, @metadata_column_family])

    %__MODULE__{
      db: db,
      log_cf: log_column_family_handle,
      metadata_cf: metadata_column_family_handle,
      write_opts: write_opts
    }
    |> set_latest_index_and_term()
  end

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
    state
    |> do_fetch_from(index, [])
    |> Enum.reverse()
  end

  defp do_fetch_from(state, index, acc) do
    case fetch(state, index) do
      {:ok, entry} ->
        do_fetch_from(state, index + 1, [entry | acc])

      :error ->
        acc
    end
  end

  @impl true
  def append(%__MODULE__{} = state, [], _at_index), do: state
  def append(%__MODULE__{} = state, entries, at_index) do
    {:ok, batch} = :rocksdb.batch()

    state =
      if at_index do
        %{state | latest_index: at_index - 1}
      else
        state
      end

    state =
      Enum.reduce(entries, state, fn entry, state ->
        index = state.latest_index + 1
        :ok = :rocksdb.batch_put(batch, state.log_cf, encode(index), encode(entry))

        %{state | latest_index: index}
      end)

    :ok = :rocksdb.write_batch(state.db, batch, state.write_opts)
    :ok = :rocksdb.release_batch(batch)

    Logger.debug("appended #{Enum.count(entries)} log entries", logger_metadata(trace: {:appended, entries}))

    %{state | latest_term: List.last(entries).term}
  end

  @impl true
  # unimplemented in optimistic mode, it seems :(
  # def rewind(%__MODULE__{latest_index: latest_index} = state, index) when index < latest_index do
    # :ok = :rocksdb.delete_range(state.db, state.log_cf, encode(index + 1), encode(state.latest_index + 1), state.write_opts)

  #   %{state | latest_index: index}
  # end
  # def rewind(state, _index), do: state

  def rewind(%__MODULE__{latest_index: latest_index} = state, index) when index < latest_index do
    {:ok, transaction} = :rocksdb.transaction(state.db, state.write_opts)
    {:ok, iterator} = :rocksdb.transaction_iterator(transaction, state.log_cf, [])

    do_rewind(transaction, iterator, state.log_cf, encode(index))

    :ok = :rocksdb.transaction_commit(transaction)

    old_latest_index = state.latest_index
    state = set_latest_index_and_term(state)

    Logger.debug(fn ->
      num_entries = old_latest_index - state.latest_index

      {"rewound #{num_entries} log entries", logger_metadata(trace: {:rewound_log, num_entries})}
    end)

    state
  end
  def rewind(%__MODULE__{} = state, _index), do: state

  defp do_rewind(transaction, iterator, log_cf, min_index) do
    case :rocksdb.iterator_move(iterator, :last) do
      {:ok, index, _value} when index > min_index ->
        :ok = :rocksdb.transaction_delete(transaction, log_cf, index)
        do_rewind(transaction, iterator, log_cf, min_index)

      {:ok, _index, _value} ->
        :ok
    end
  end

  @impl true
  # the current version of rocksdb-erlang doesn't support delete_range in transactions, so we have to do it with an iterator
  # in order to maintain atomicity with the snapshot_entry insertion
  #
  # https://github.com/facebook/rocksdb/issues/4812
  def truncate(%__MODULE__{} = state, index, snapshot_entry) do
    {:ok, transaction} = :rocksdb.transaction(state.db, state.write_opts)
    {:ok, iterator} = :rocksdb.transaction_iterator(transaction, state.log_cf, [])

    do_truncate(transaction, iterator, state.log_cf, encode(index))

    :ok = :rocksdb.transaction_put(transaction, state.log_cf, encode(index), encode(snapshot_entry))
    :ok = :rocksdb.transaction_commit(transaction)

    set_latest_index_and_term(state)
  end

  defp do_truncate(transaction, iterator, log_cf, max_index) do
    case :rocksdb.iterator_move(iterator, :first) do
      {:ok, index, _value} when index <= max_index ->
        :ok = :rocksdb.transaction_delete(transaction, log_cf, index)
        do_truncate(transaction, iterator, log_cf, max_index)

      {:ok, _index, _value} ->
        :ok

      {:error, :invalid_iterator} ->
        :ok
    end
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
    {:ok, iterator} = :rocksdb.iterator(state.db, state.log_cf, [])
    {:ok, first_index, _} = :rocksdb.iterator_move(iterator, :first)
    {:ok, last_index, _} = :rocksdb.iterator_move(iterator, :last)
    :ok = :rocksdb.iterator_close(iterator)

    decode(last_index) - decode(first_index)
  end

  @impl true
  def put_metadata(%__MODULE__{} = state, metadata) do
    dumped =
      metadata
      |> Map.from_struct()
      |> encode()

    :ok = :rocksdb.put(state.db, state.metadata_cf, "metadata", dumped, state.write_opts)

    state
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
  def close(%__MODULE__{} = state) do
    :rocksdb.close(state.db)
  end

  @impl true
  def dump(%__MODULE__{} = state) do
    Enum.flat_map([state.metadata_cf, state.log_cf], fn cf ->
      {:ok, iterator} = :rocksdb.iterator(state.db, cf, [])
      {:ok, index, value} = :rocksdb.iterator_move(iterator, :first)

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
    end)
  end

  defp encode(term), do: :erlang.term_to_binary(term)
  defp decode(binary), do: :erlang.binary_to_term(binary)

  defp set_latest_index_and_term(%__MODULE__{} = state) do
    {latest_index, latest_term} =
      with {:ok, iterator} <- :rocksdb.iterator(state.db, state.log_cf, []),
           {:ok, index, entry} <- :rocksdb.iterator_move(iterator, :last) do
        :ok = :rocksdb.iterator_close(iterator)
        {decode(index), decode(entry).term}
      else
        {:error, :invalid_iterator} ->
          {-1, -1}
      end

    %{state | latest_index: latest_index, latest_term: latest_term}
  end
end
