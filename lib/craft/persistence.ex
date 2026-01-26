defmodule Craft.Persistence do
  @moduledoc false

  alias Craft.Log.EmptyEntry
  alias Craft.Log.CommandEntry
  alias Craft.Log.MembershipEntry
  alias Craft.Log.SnapshotEntry

  # this module is kinda like a bastardized mix of a behaviour and a protocol
  #
  # it's a behaviour in the sense that:
  # we want the user to hand us a module name as an option, but we don't want them
  # to instantiate it for us, we want to do that at init-time for the consensus process
  #
  # but it's a protocol in the sense that:
  # it'd just be nice to call (e.g.) Persistence.latest_term(t()) and not have to carry the module
  # name around with us and wrap/unwrap it
  #
  # so yeah, if you have a better idea how to do this, holler at me please. :)
  #

  @type entry :: EmptyEntry.t() | CommandEntry.t() | MembershipEntry.t() | SnapshotEntry.t()

  #TODO: proper typespecs
  @callback new(group_name :: String.t(), args :: any()) :: any()
  @callback latest_term(any()) :: integer()
  @callback latest_index(any()) :: integer()
  @callback fetch(any(), index :: integer()) :: entry()
  @callback fetch_from(any(), index :: integer()) :: [entry()]
  @callback fetch_between(any(), index_range :: Range.t()) :: [entry(), ...] # increasing range
  @callback append(any(), [entry()]) :: any()
  @callback rewind(any(), index :: integer()) :: any() # remove all long entries after index
  @callback truncate(any(), index :: integer(), SnapshotEntry.t()) :: any() # atomically remove log entries up to and including `index` and replace with SnapshotEntry
  @callback reverse_find(any(), fun()) :: entry() | nil
  @callback reduce_while(any(), any(), fun()) :: any()
  @callback put_metadata(any(), struct()) :: any()
  @callback fetch_metadata(any()) :: {:ok, binary()} | :error

  # commit_buffer/1 should also write out the metadata:
  #   lease needs to be fsync'd with every AppendEntries, fsyncs are expensive, this adds it to the buffer so we only have to do one:
  #   1. on the leader when it exteneds its lease before a heartbeat
  #   2. on a follower when it updates the lease as part of AppendEntries receipt
  @callback any_buffered_log_writes?(any()) :: boolean()
  @callback buffer_append(any(), entry()) :: {any(), index :: integer()}
  @callback buffer_rewind(any(), index :: integer()) :: any()
  @callback buffer_metadata_put(any(), metadata :: struct()) :: any()
  @callback commit_buffer(any()) :: any()
  @callback release_buffer(any()) :: any()

  @callback dump(any()) :: any()
  @callback length(any()) :: pos_integer()
  @callback backup(any(), Path.t()) :: :ok | {:error, any()}
  @callback close(any()) :: :ok

  @optional_callbacks [close: 1]

  defstruct [
    :module,
    :private
  ]

  defmodule Metadata do
    alias Craft.Consensus.State

    defstruct [
      :current_term,
      :voted_for,
      :lease_expires_at
    ]

    def new(%State{} = state) do
      %{
        current_term: state.current_term,
        voted_for: state.voted_for,
        lease_expires_at: state.lease_expires_at
      }
    end

    def load(%State{} = state) do
      case state.persistence.module.fetch_metadata(state.persistence.private) do
        {:ok, %__MODULE__{} = metadata} ->
          %{
            state |
              current_term: metadata.current_term,
              voted_for: metadata.voted_for,
              lease_expires_at: metadata.lease_expires_at
          }

        :error ->
          state
      end
    end

    def write(%State{} = state) do
      put_in(
        state.persistence.private,
        state.persistence.module.put_metadata(state.persistence.private, new(state))
      )
    end

    def buffer_put(%State{} = state) do
      put_in(
        state.persistence.private,
        state.persistence.module.buffer_metadata_put(state.persistence.private, new(state))
      )
    end
  end

  #
  # Craft initializes the log with a starter entry, like so:
  #
  # log: 0 -> EmptyEntry{term: -1}
  #
  # this makes the rest of the codebase a lot simpler
  #
  def new(group_name, {module, args}) do
    persistence =
      %__MODULE__{
        module: module,
        private: module.new(group_name, args)
      }

    if first(persistence) do
      persistence
    else
      append(persistence, %EmptyEntry{term: -1})
    end
  end

  def new(group_name, module) when not is_nil(module) do
    new(group_name, {module, []})
  end

  # Log

  def latest_term(%__MODULE__{module: module, private: private}) do
    module.latest_term(private)
  end

  def latest_index(%__MODULE__{module: module, private: private}) do
    module.latest_index(private)
  end

  def fetch(%__MODULE__{module: module, private: private}, index) do
    module.fetch(private, index)
  end

  def fetch_from(%__MODULE__{module: module, private: private}, index) do
    module.fetch_from(private, index)
  end

  def fetch_between(%__MODULE__{module: module, private: private}, index_range) do
    module.fetch_between(private, index_range)
  end

  def any_buffered_log_writes?(%__MODULE__{module: module, private: private}) do
    module.any_buffered_log_writes?(private)
  end

  def buffer_append(%__MODULE__{module: module, private: private} = persistence, entry) do
    {private, index} = module.buffer_append(private, entry)

    {%{persistence | private: private}, index}
  end

  def buffer_rewind(%__MODULE__{module: module, private: private} = persistence, index) do
    %{persistence | private: module.buffer_rewind(private, index)}
  end

  def buffer_metadata_put(%__MODULE__{module: module, private: private} = persistence, metadata) do
    %{persistence | private: module.buffer_metadata_put(private, metadata)}
  end

  def commit_buffer(%__MODULE__{module: module, private: private} = persistence) do
    %{persistence | private: module.commit_buffer(private)}
  end

  def release_buffer(%__MODULE__{module: module, private: private} = persistence) do
    %{persistence | private: module.release_buffer(private)}
  end

  def append(%__MODULE__{module: module, private: private} = persistence, entry) do
    %{persistence | private: module.append(private, entry)}
  end

  def rewind(%__MODULE__{module: module, private: private} = persistence, index) do
    %{persistence | private: module.rewind(private, index)}
  end

  def truncate(%__MODULE__{module: module, private: private} = persistence, index, %SnapshotEntry{} = snapshot_entry) do
    %{persistence | private: module.truncate(private, index, snapshot_entry)}
  end

  def reverse_find(%__MODULE__{module: module, private: private}, fun) do
    module.reverse_find(private, fun)
  end

  def reduce_while(%__MODULE__{module: module, private: private}, initial_value, fun) do
    module.reduce_while(private, initial_value, fun)
  end

  # probably should just defer this to the module rather than getting cute with an iterator
  def first(%__MODULE__{} = persistence) do
    reduce_while(persistence, nil, fn {index, entry}, nil -> {:halt, {index, entry}} end)
  end

  def length(%__MODULE__{module: module, private: private}) do
    module.length(private)
  end

  def backup(%__MODULE__{module: module, private: private}, to_directory) do
    module.backup(private, to_directory)
  end

  def close(%__MODULE__{module: module, private: private} = persistence) do
    %{persistence | private: module.close(private)}
  end

  def dump(%__MODULE__{module: module, private: private}) do
    module.dump(private)
  end
end
