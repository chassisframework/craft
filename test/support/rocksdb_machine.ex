defmodule Craft.RocksDBMachine do
  use Craft.Machine, mutable: true

  alias Craft.Configuration

  @log_index_column_family {~c"log_index", []}
  @log_index_key "log_index"

  def put(name, k, v) do
    Craft.command({:put, k, v}, name)
  end

  def get(name, k) do
    Craft.query({:get, k}, name)
  end

  defmodule State do
    defstruct [:data_dir, :db, :log_index_column_family, :snapshots_dir]
  end

  @impl true
  def init(group_name) do
    group_dir =
      group_name
      |> Configuration.find()
      |> Map.fetch!(:data_dir)

    data_dir = Path.join([Configuration.data_dir(), group_dir, "machine"])

    File.mkdir_p!(data_dir)

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

    {:ok, %{state | db: db, log_index_column_family: log_index_column_family}}
  end

  @impl true
  def receive_snapshot(state) do
    {:ok, state} = do_init(state)

    state
  end

  @impl true
  def prepare_to_receive_snapshot(state) do
    :rocksdb.close(state.db)

    File.rm_rf!(state.data_dir)

    {:ok, state.data_dir, state}
  end

  @impl true
  def handle_command({:put, k, v}, log_index, state) do
    {:ok, batch} = :rocksdb.batch()
    :ok = :rocksdb.batch_put(batch, encode(k), encode(v))
    :ok = :rocksdb.batch_put(batch, state.log_index_column_family, @log_index_key, encode(log_index))
    :ok = :rocksdb.write_batch(state.db, batch, sync: true)
    :ok = :rocksdb.release_batch(batch)

    {:ok, state}
  end

  # TODO: inject this clause via `use Craft.Machine`
  def handle_command(_, _log_index, state) do
    {{:error, :unknown_command}, state}
  end

  @impl true
  def handle_query({:get, k}, _from, state) do
    resp =
      case :rocksdb.get(state.db, encode(k), []) do
        {:ok, value} -> {:ok, decode(value)}
        :not_found -> {:error, :not_found}
        error -> error
      end

    {:reply, resp}
  end

  def handle_query(_, _from, state) do
    {:reply, {{:error, :unknown_query}, state}}
  end

  @impl true
  def handle_role_change(_new_role, state) do
    state
  end

  @impl true
  def snapshot(state) do
    index =
      state
      |> last_applied_log_index()
      |> to_string()

    path = Path.join(state.snapshots_dir, index)

    if not File.exists?(path) do
      File.mkdir_p!(state.snapshots_dir)

      :ok = :rocksdb.checkpoint(state.db, :erlang.binary_to_list(path))

      {last_applied_log_index(state), path, state}
    else
      nil
    end
  end

  @impl true
  def snapshots(state) do
    state.snapshots_dir
    |> File.ls!()
    |> Map.new(fn index_str ->
      {index, ""} = Integer.parse(index_str)

      {index, Path.join(state.snapshots_dir, index_str)}
    end)
  end

  @impl true
  def last_applied_log_index(state) do
    case :rocksdb.get(state.db, state.log_index_column_family, @log_index_key, []) do
      {:ok, value} ->
        decode(value)

      :not_found ->
        0
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

    snapshots = File.ls!(state.snapshots_dir)

    %{kv: kv, last_applied_log_index: last_applied_log_index(state), snapshots: snapshots}
  end

  defp encode(term), do: :erlang.term_to_binary(term)
  defp decode(binary), do: :erlang.binary_to_term(binary)
end
