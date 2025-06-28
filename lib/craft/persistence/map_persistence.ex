defmodule Craft.Persistence.MapPersistence do
  @moduledoc """
  In-memory log, do not use in production.
  """
  @behaviour Craft.Persistence

  @impl true
  def new(_group_name, []) do
    {%{}, nil}
  end

  @impl true
  def latest_term({log, _metadata} = state) do
    log
    |> Map.fetch!(latest_index(state))
    |> Map.fetch!(:term)
  end

  @impl true
  def latest_index({log, _metadata}) do
    map_size(log) - 1
  end

  @impl true
  def fetch({log, _metadata}, index) do
    Map.fetch(log, index)
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
  def append(state, [], _at_index), do: state

  def append({log, _metadata} = state, entries, nil) do
    append(state, entries, map_size(log))
  end

  def append({log, metadata}, [entry | rest], at_index) do
    log = Map.put(log, at_index, entry)

    append({log, metadata}, rest, at_index + 1)
  end

  @impl true
  def rewind({log, metadata} = state, index) when index + 1 < map_size(log) do
    log = Map.delete(log, latest_index(state))

    rewind({log, metadata}, index)
  end
  def rewind(state, _index), do: state

  @impl true
  def truncate({log, metadata}, snapshot_index, snapshot_entry) do
    log =
      log
      |> Map.filter(fn
        {index, _entry} when index < snapshot_index ->
          false

        pair ->
          pair
      end)
      |> Map.put(snapshot_index, snapshot_entry)

    {log, metadata}
  end

  @impl true
  def reverse_find({log, _metadata} = state, fun) do
    Enum.find_value(latest_index(log)..0, fn i ->
      {:ok, entry} = fetch(state, i)

      if fun.(entry) do
        entry
      else
        false
      end
    end)
  end

  @impl true
  def reduce_while({log, _metadata}, acc, fun) do
    Enum.reduce_while(log, acc, fun)
  end

  @impl true
  def length({log, _metadata}) do
    map_size(log)
  end

  @impl true
  def put_metadata({log, _old_metadata}, metadata) do
    {log, metadata}
  end

  @impl true
  def fetch_metadata({_log, nil}) do
    :error
  end

  def fetch_metadata({_log, metadata}) do
    {:ok, metadata}
  end

  @impl true
  def close(state) do
    state
  end

  @impl true
  def dump(state) do
    state
  end
end
