defmodule Craft.MemberSupervisor do
  @moduledoc false

  use Supervisor

  alias Craft.Persistence.RocksDBPersistence

  import Craft.Application, only: [via: 2, lookup: 2]

  @consensus_module Application.compile_env(:craft, :consensus_module, Craft.Consensus)

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: via(args.name, __MODULE__))
  end

  @impl Supervisor
  def init(args) do
    children = [
      {@consensus_module, args},
      {Craft.Machine, args}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  # TODO: - :data_dir implies {RocksDBPersistence, data_dir: data_dir}
  #       - allow passing :persistence key has a {module, args} tuple
  def start_member(name, nodes, machine, opts \\ []) do
    defaults = [
      name: name,
      nodes: nodes,
      machine: machine,
      persistence: {RocksDBPersistence, []}
    ]

    args =
      case opts do
        # for testing
        %{consensus_data: consensus_data, opts: opts} ->
          args = opts |> Keyword.merge(defaults) |> Map.new()
          Map.put(args, :consensus_data, consensus_data)

        _ ->
          opts |> Keyword.merge(defaults) |> Map.new()
      end

    case DynamicSupervisor.start_child(Craft.Supervisor, {__MODULE__, args}) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end

  def stop_member(name) do
    DynamicSupervisor.terminate_child(Craft.Supervisor, lookup(name, __MODULE__))
  end
end
