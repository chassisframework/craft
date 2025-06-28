defmodule Craft.MemberSupervisor do
  @moduledoc false

  use Supervisor

  alias Craft.Persistence.RocksDBPersistence
  alias Craft.Configuration

  import Craft.Application, only: [via: 2, lookup: 2]

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: via(args.name, __MODULE__))
  end

  @impl Supervisor
  def init(args) do
    children = [
      {Craft.Machine, args},
      {Craft.Consensus, args}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  def start_member(name) do
    case Configuration.find(name) do
      config when is_map(config) ->
        do_start_member(config)

      _ ->
        {:error, :not_found}
    end
  end

  def start_member(name, opts) do
    opts =
      Map.merge(%{
        name: name,
        persistence: {RocksDBPersistence, []}
      }, opts)

    if Configuration.find(name) do
      {:error, :already_exists}
    else
      config = %{
        machine: opts.machine,
        persistence: opts.persistence,
        nodes: opts.nodes,
        global_clock: opts[:global_clock]
      }

      Configuration.write_new!(name, config)

      do_start_member(opts)
    end
  end

  defp do_start_member(opts) do
    case DynamicSupervisor.start_child(Craft.Supervisor, {__MODULE__, opts}) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end

  def stop_member(name) do
    Craft.MemberCache.delete(name)
    DynamicSupervisor.terminate_child(Craft.Supervisor, lookup(name, __MODULE__))
  end
end
