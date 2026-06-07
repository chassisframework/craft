defmodule Craft.Persistence.MapPersistence do
  @moduledoc """
  This module should not be used in production, it does not write to disk.
  """
  @behaviour Craft.Persistence

  require Logger

  import Craft.Tracing, only: [logger_metadata: 1]


  # write buffer stores an ordered list of operations
  defstruct [
    :write_buffer,
    :buffered_metadata,
    :metadata,
    entries: %{},
    earliest_index: 0,
    latest_index: 0,
  ]

  @impl true
  def new(_group_name, _opts \\ []) do
    %__MODULE__{} |> release_buffer()
  end

  @impl true
  def latest_index(%__MODULE__{} = state), do: state.latest_index

  @impl true
  def latest_term(%__MODULE__{} = state) do
    case state.entries[state.latest_index] do
      nil ->
        -1

      %{term: term} ->
        term
    end
  end

  @impl true
  def fetch(%__MODULE__{} = state, index) do
    Map.fetch(state.entries, index)
  end

  @impl true
  def fetch_from(%__MODULE__{} = state, index) do
    fetch_between(state, index..latest_index(state)//1)
  end

  @impl true
  def fetch_between(%__MODULE__{} = state, index_range) do
    Enum.map(index_range, &Map.fetch!(state.entries, &1))
  end

  @impl true
  def any_buffered_log_writes?(%__MODULE__{} = state) do
    state.write_buffer != []
  end

  @impl true
  def buffer_append(%__MODULE__{} = state, %_struct{} = entry) do
    index =
      case List.first(state.write_buffer) do
        {:append, index, _entry} ->
          index

        {:rewind, index} ->
          index

        nil ->
          latest_index(state)
      end

    index = index + 1
    operation = {:append, index, entry}

    Logger.debug("appended log entry to Map batch", logger_metadata(trace: operation))

    {%{state | write_buffer: [operation | state.write_buffer]}, index}
  end

  @impl true
  def buffer_rewind(%__MODULE__{} = state, index) do
    operation = {:rewind, index}

    Logger.debug("appended rewind to Map batch", logger_metadata(trace: operation))

    %{state | write_buffer: [operation | state.write_buffer]}
  end

  @impl true
  def buffer_metadata_put(%__MODULE__{} = state, metadata) do
    %{state | buffered_metadata: metadata}
  end

  @impl true
  def commit_buffer(%__MODULE__{} = state) do
    state =
      state.write_buffer
      |> Enum.reverse()
      |> Enum.reduce(state, fn
        {:append, index, entry}, state ->
          earliest_index =
            if state.entries == %{} do
              index
            else
              state.earliest_index
            end
          %{state | earliest_index: earliest_index, latest_index: index, entries: Map.put(state.entries, index, entry)}

        {:rewind, index}, state ->
          %{state | latest_index: index, entries: Enum.reduce(index+1..latest_index(state)//1, state.entries, &Map.delete(&2, &1))}
      end)

    if state.buffered_metadata do
      %{state | metadata: state.buffered_metadata}
    else
      state
    end
    |> release_buffer()
  end

  @impl true
  def release_buffer(%__MODULE__{} = state) do
    %{state | buffered_metadata: nil, write_buffer: []}
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
    entries = Enum.reduce(state.earliest_index..index//1, state.entries, &Map.delete(&2, &1))

    %{state | entries: Map.put(entries, index, snapshot_entry), earliest_index: index}
  end

  @impl true
  def reverse_find(%__MODULE__{} = state, fun) do
    index = Enum.find(latest_index(state)..state.earliest_index//-1, fn index -> fun.(state.entries[index]) end)

    if index do
      state.entries[index]
    end
  end

  @impl true
  def reduce_while(%__MODULE__{entries: entries}, acc, _fun) when map_size(entries) == 0, do: acc
  def reduce_while(%__MODULE__{} = state, acc, fun) do
    Enum.reduce_while(state.earliest_index..latest_index(state)//1, acc, fn index, acc ->
      fun.({index, state.entries[index]}, acc)
    end)
  end

  @impl true
  def length(%__MODULE__{} = state) do
    map_size(state.entries)
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
  def close(_state) do
    :ok
  end

  @impl true
  def dump(%__MODULE__{} = state) do
    %{
      log: Enum.map(state.earliest_index..latest_index(state)//1, fn i -> {i, state.entries[i]} end),
      write_buffer: state.write_buffer,
      buffered_metadata: state.buffered_metadata,
      metadata: state.metadata
    }
  end
end
