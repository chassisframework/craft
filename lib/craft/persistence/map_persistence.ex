defmodule Craft.Persistence.MapPersistence do
  @moduledoc """
  In-memory log, do not use in production.
  """
  @behaviour Craft.Persistence

  # it's not necessary to use a write buffer for this memory-only log,
  # but it roughly simulates the behaviour of the on-disk implementation
  # so it may turn up bugs
  defstruct [:metadata, log: %{}, append_buffer: %{}]

  @impl true
  def new(_group_name, []) do
    %__MODULE__{}
  end

  @impl true
  def latest_term(state) do
    state.log
    |> Map.fetch!(latest_index(state))
    |> Map.fetch!(:term)
  end

  @impl true
  def latest_index(state) do
    map_size(state.log) - 1
  end

  @impl true
  def fetch(state, index) do
    Map.fetch(state.log, index)
  end

  @impl true
  def fetch_from(state, index) do
    if index > latest_index(state) do
      []
    else
      Enum.map(index..latest_index(state), fn index ->
        {:ok, entry} = fetch(state, index)

        entry
      end)
    end
  end

  @impl true
  def fetch_between(state, index_range) do
    Enum.map(index_range, fn index ->
      {:ok, entry} = fetch(state, index)

      entry
    end)
  end

  @impl true
  def append(state, []), do: state

  def append(state, [entry | rest]) do
    put_in(state.log[map_size(state.log)], entry) |> append(rest)
  end

  @impl true
  def add_to_append_buffer(state, entry) do
    put_in(state.append_buffer[map_size(state.log)], entry)
  end

  @impl true
  def write_append_buffer(%__MODULE__{} = state) do
    log = Map.merge(state.log, state.append_buffer)

    %{state | log: log}
  end

  @impl true
  def release_append_buffer(%__MODULE__{} = state) do
    %{state | append_buffer: %{}}
  end

  @impl true
  def rewind(state, index) when index + 1 < map_size(state.log) do
    log = Map.delete(state.log, latest_index(state))

    rewind(%{state | log: log}, index)
  end
  def rewind(state, _index), do: state

  @impl true
  def truncate(state, snapshot_index, snapshot_entry) do
    log =
      state.log
      |> Map.filter(fn
        {index, _entry} when index < snapshot_index ->
          false

        pair ->
          pair
      end)
      |> Map.put(snapshot_index, snapshot_entry)

    %{state | log: log}
  end

  @impl true
  def reverse_find(state, fun) do
    Enum.find_value(latest_index(state)..0, fn i ->
      {:ok, entry} = fetch(state, i)

      if fun.(entry) do
        entry
      else
        false
      end
    end)
  end

  @impl true
  def reduce_while(state, acc, fun) do
    Enum.reduce_while(state.log, acc, fun)
  end

  @impl true
  def length(state) do
    map_size(state.log)
  end

  @impl true
  def put_metadata(state, metadata) do
    %{state | metadata: metadata}
  end

  @impl true
  def fetch_metadata(state) do
    if state.metadata do
      {:ok, state.metadata}
    else
      :error
    end
  end

  @impl true
  def close(state) do
    state
  end

  @impl true
  def dump(state) do
    state
  end

  @impl true
  def backup(_dir, _state) do
    :ok
  end
end
