defmodule Craft.Leases do
  @moduledoc false

  # this is the authoritative local source of lease information, MemberCache is similar, but it can overwrite its contents
  # when it polls foreign nodes, leading to inconsistencies. this module is strictly node-local.

  require Record

  alias Craft.Consensus.State
  alias Craft.GlobalTimestamp
  alias Craft.MemberCache

  defmodule Lease do
    @moduledoc false
    defstruct [:group_name, :node, :module, :expires_at]
  end
  Record.defrecord(:lease, [:group_name, :node, :module, :expires_at])

  def create_table do
    :ets.new(__MODULE__, [:set, :named_table, :public, read_concurrency: true])
  end

  @doc false
  def all do
    :ets.foldr(& [&1 | &2], [], __MODULE__)
    |> Map.new(fn record -> {elem(record, 0), new(record)} end)
  end

  @doc false
  def get(group_name) do
    case :ets.lookup(__MODULE__, group_name) do
      [tuple] ->
        {:ok, new(tuple)}

      [] ->
        :not_found
    end
  end

  @doc false
  def holding_lease?(group_name) do
    with {:ok, %Lease{node: node, module: module, expires_at: expires_at}} when node == node() <- get(group_name),
         {:ok, time} when time > 0 <- GlobalTimestamp.time_until_lease_expires(module, expires_at) do
      true
    else
      _ ->
        false
    end
  end

  @doc false
  def update(%State{} = state, expires_at) do
    MemberCache.update_lease_holder(state, expires_at)

    tuple =
      lease(node: node(),
            module: state.global_clock,
            expires_at: expires_at)
      |> put_elem(0, state.name)

    :ets.insert(__MODULE__, tuple)
  end

  @doc false
  def delete(group_name) do
    :ets.delete(__MODULE__, group_name)
  end

  defp new(record) do
    %Lease{
      group_name: elem(record, 0),
      node: lease(record, :node),
      module: lease(record, :module),
      expires_at: lease(record, :expires_at)
    }
  end
end
