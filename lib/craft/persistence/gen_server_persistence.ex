defmodule Craft.Persistence.GenServerPersistence do
  @moduledoc """
  This module should not be used in production, it does not write to disk.
  """
  use GenServer

  alias Craft.Persistence
  alias Craft.Persistence.MapPersistence
  @behaviour Persistence

  # require Logger

  # import Craft.Tracing, only: [logger_metadata: 1]

  @impl Persistence
  def new(_group_name, _opts \\ []) do
    {:ok, pid} = GenServer.start_link(__MODULE__, [])

    pid
  end

  @impl Persistence
  def latest_index(pid), do: GenServer.call(pid, :latest_index)
  @impl Persistence
  def latest_term(pid), do: GenServer.call(pid, :latest_term)
  @impl Persistence
  def fetch(pid, index), do: GenServer.call(pid, {:fetch, index})
  @impl Persistence
  def fetch_from(pid, index), do: GenServer.call(pid, {:fetch_from, index})
  @impl Persistence
  def fetch_between(pid, index_range), do: GenServer.call(pid, {:fetch_between, index_range})
  @impl Persistence
  def any_buffered_log_writes?(pid), do: GenServer.call(pid, :any_buffered_log_writes?)
  @impl Persistence
  def buffer_append(pid, entry), do: GenServer.call(pid, {:buffer_append, entry})
  @impl Persistence
  def buffer_rewind(pid, index), do: GenServer.call(pid, {:buffer_rewind, index})
  @impl Persistence
  def buffer_metadata_put(pid, metadata), do: GenServer.call(pid, {:buffer_metadata_put, metadata})
  @impl Persistence
  def commit_buffer(pid), do: GenServer.call(pid, :commit_buffer)
  @impl Persistence
  def release_buffer(pid), do: GenServer.call(pid, :release_buffer)
  @impl Persistence
  def append(pid, entry), do: GenServer.call(pid, {:append, entry})
  @impl Persistence
  def rewind(pid, index), do: GenServer.call(pid, {:rewind, index})
  @impl Persistence
  def truncate(pid, index, snapshot_entry), do: GenServer.call(pid, {:truncate, index, snapshot_entry})
  @impl Persistence
  def reverse_find(pid, fun), do: GenServer.call(pid, {:reverse_find, fun})
  @impl Persistence
  def reduce_while(pid, acc, fun), do: GenServer.call(pid, {:reduce_while, acc, fun})
  @impl Persistence
  def length(pid), do: GenServer.call(pid, :length)
  @impl Persistence
  def put_metadata(pid, metadata), do: GenServer.call(pid, {:put_metadata, metadata})
  @impl Persistence
  def fetch_metadata(pid), do: GenServer.call(pid, :fetch_metadata)
  @impl Persistence
  def backup(_pid, _to_directory), do: :ok
  @impl Persistence
  def close(pid), do: GenServer.call(pid, :close)
  @impl Persistence
  def dump(pid), do: GenServer.call(pid, :dump)

  @impl GenServer
  def init([]) do
    {:ok, MapPersistence.new(nil)}
  end

  @impl GenServer
  def handle_call(:latest_index, _from, state), do: {:reply, MapPersistence.latest_index(state), state}
  def handle_call(:latest_term, _from, state), do: {:reply, MapPersistence.latest_term(state), state}
  def handle_call({:fetch, index}, _from, state), do: {:reply, MapPersistence.fetch(state, index), state}
  def handle_call({:fetch_from, index}, _from, state) do
    result =
      if index > MapPersistence.latest_index(state) do
        []
      else
        MapPersistence.fetch_from(state, index)
      end

    {:reply, result, state}
  end
  def handle_call({:fetch_between, index_range}, _from, state), do: {:reply, MapPersistence.fetch_between(state, index_range), state}
  def handle_call(:any_buffered_log_writes?, _from, state), do: {:reply, MapPersistence.any_buffered_log_writes?(state), state}

  def handle_call({:buffer_append, entry}, _from, state) do
    {state, index} = MapPersistence.buffer_append(state, entry)

    {:reply, {self(), index}, state}
  end

  def handle_call({:buffer_rewind, index}, _from, state) do
    state =
      if index > MapPersistence.latest_index(state) do
        state
      else
        MapPersistence.buffer_rewind(state, index)
      end

    {:reply, self(), state}
  end

  def handle_call({:buffer_metadata_put, metadata}, _from, state), do: {:reply, self(), MapPersistence.buffer_metadata_put(state, metadata)}
  def handle_call(:commit_buffer, _from, state), do: {:reply, self(), MapPersistence.commit_buffer(state)}
  def handle_call(:release_buffer, _from, state), do: {:reply, self(), MapPersistence.release_buffer(state)}
  def handle_call({:append, entry}, _from, state), do: {:reply, self(), MapPersistence.append(state, entry)}
  def handle_call({:rewind, index}, _from, state), do: {:reply, self(), MapPersistence.rewind(state, index)}
  def handle_call({:truncate, index, snapshot_entry}, _from, state), do: {:reply, self(), MapPersistence.truncate(state, index, snapshot_entry)}
  def handle_call({:reverse_find, fun}, _from, state), do: {:reply, MapPersistence.reverse_find(state, fun), state}
  def handle_call({:reduce_while, acc, fun}, _from, state), do: {:reply, MapPersistence.reduce_while(state, acc, fun), state}
  def handle_call(:length, _from, state), do: {:reply, MapPersistence.length(state), state}
  def handle_call({:put_metadata, metadata}, _from, state), do: {:reply, self(), MapPersistence.put_metadata(state, metadata)}
  def handle_call(:fetch_metadata, _from, state), do: {:reply, MapPersistence.fetch_metadata(state), state}
  def handle_call(:close, _from, state), do: {:stop, :normal, :ok, MapPersistence.close(state)}
  def handle_call(:dump, _from, state), do: {:reply, MapPersistence.dump(state), state}
end
