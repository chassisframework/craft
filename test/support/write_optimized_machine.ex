defmodule Craft.WriteOptimizedMachine do
  use Craft.Machine,
      mutable: true,
      write_optimized: true

  alias Craft.RocksDBMachine

  defdelegate put(name, k, v), to: RocksDBMachine
  defdelegate put_async(name, k, v), to: RocksDBMachine
  defdelegate get(name, k), to: RocksDBMachine

  @impl true
  defdelegate init(args), to: RocksDBMachine

  @impl true
  defdelegate receive_snapshot(state), to: RocksDBMachine

  @impl true
  defdelegate prepare_to_receive_snapshot(state), to: RocksDBMachine

  @impl true
  defdelegate handle_commands(commands, state), to: RocksDBMachine

  @impl true
  defdelegate handle_query(query, from, state), to: RocksDBMachine

  @impl true
  defdelegate handle_role_change(new_role, state), to: RocksDBMachine

  @impl true
  defdelegate snapshot(state), to: RocksDBMachine

  @impl true
  defdelegate snapshots(state), to: RocksDBMachine

  @impl true
  defdelegate last_applied_log_index(state), to: RocksDBMachine

  @impl true
  defdelegate backup(to_directory, state), to: RocksDBMachine
end
