defmodule Craft.RocksDBMachine do
  use Craft.Machine, mutable: true

  @log_index_column_family {~c"log_index", []}
  @log_index_key "log_index"

  def put(name, k, v) do
    Craft.command({:put, k, v}, name)
  end

  def get(name, k) do
    Craft.command({:get, k}, name)
  end

  defmodule State do
    defstruct [:data_dir, :db, :log_index_column_family, :snapshots_dir]
  end

  def init(group_name) do
    data_dir = Path.join([File.cwd!(), "data", to_string(node()), group_name, "machine"])

    %State{
      data_dir: data_dir,
      snapshots_dir: Path.join(data_dir, "snapshots")
    }
    |> do_init(create_if_missing: true, create_missing_column_families: true)
  end

  defp do_init(state, db_opts \\ []) do
    {:ok, db, [_default, log_index_column_family]} =
      state.data_dir
      |> :erlang.binary_to_list()
      |> :rocksdb.open_optimistic_transaction_db(db_opts, [{~c"default", []}, @log_index_column_family])

    File.mkdir_p!(state.snapshots_dir)

    {:ok, %State{state | db: db, log_index_column_family: log_index_column_family}}
  end

  def receive_snapshot(state) do
    do_init(state)
  end

  def prepare_to_receive_snapshot(state) do
    :ok = :rocksdb.close(state.db)

    File.rm_rf!(state.data_dir)

    {:ok, state.data_dir, state}
  end

  def command({:put, k, v}, log_index, state) do
    {:ok, batch} = :rocksdb.batch()
    :ok = :rocksdb.batch_put(batch, encode(k), encode(v))
    :ok = :rocksdb.batch_put(batch, state.log_index_column_family, @log_index_key, encode(log_index))
    :ok = :rocksdb.write_batch(state.db, batch, sync: true)
    :ok = :rocksdb.release_batch(batch)

    {:ok, state}
  end

  def command({:get, k}, log_index, state) do
    :ok = :rocksdb.put(state.db, state.log_index_column_family, @log_index_key, encode(log_index), sync: true)

    case :rocksdb.get(state.db, encode(k), []) do
      {:ok, value} ->
        {{:ok, decode(value)}, state}

      :not_found ->
        {{:error, :not_found}, state}

      error ->
        {error, state}
    end
  end

  # TODO: inject this clause via `use Craft.Machine`
  def command(_, _log_index, state) do
    {{:error, :unknown_command}, state}
  end

  def snapshot(at_index, state) do
    path = Path.join(state.snapshots_dir, to_string(at_index))

    File.mkdir_p!(state.snapshots_dir)
    :ok = :rocksdb.checkpoint(state.db, :erlang.binary_to_list(path))

    path
  end

  def snapshots(state) do
    File.ls!(state.snapshots_dir)
  end

  def last_applied_log_index(state) do
    case :rocksdb.get(state.db, state.log_index_column_family, @log_index_key, []) do
      {:ok, value} ->
        decode(value)

      :not_found ->
        nil
    end
  end

  def dump(state) do
    kv =
      with {:ok, iterator} <- :rocksdb.iterator(state.db, []),
           {:ok, index, value} <- :rocksdb.iterator_move(iterator, :first) do
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
      else
        {:error, :invalid_iterator} ->
          %{}
      end

    %{kv: kv, last_applied_log_index: last_applied_log_index(state), snapshots: snapshots(state)}
  end

  defp encode(term), do: :erlang.term_to_binary(term)
  defp decode(binary), do: :erlang.binary_to_term(binary)
end
