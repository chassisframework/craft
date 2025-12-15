defmodule Craft.Application do
  @moduledoc false

  use Application

  @impl Application
  def start(_type, _args) do
    silence_sasl_logger()
    set_nexus_logger()

    children = [
      Craft.SnapshotServer,
      {Task.Supervisor, name: Craft.SnapshotServer.Supervisor},
      {DynamicSupervisor, [strategy: :one_for_one, name: Craft.Supervisor]},
      {Registry, keys: :unique, name: Craft.Registry},
      Craft.MemberCache
    ]

    # using Craft.backend/0 causes a typing error, annoying.
    backend_children =
      case Application.get_env(:craft, :backend) do
        {Craft.Sandbox, _args} ->
          [Craft.Sandbox.Manager]

        Craft.Sandbox ->
          [Craft.Sandbox.Manager]

        _ ->
          []
      end

    {:ok, pid} = Supervisor.start_link(children ++ backend_children, strategy: :rest_for_one)

    # lazy-load in dev, annoying.
    {:module, _} = Code.ensure_loaded(Craft.backend())
    if function_exported?(Craft.backend(), :init, 0) do
      :erlang.apply(Craft.backend(), :init, [])
    end

    {:ok, pid}
  end

  def via(name, component) do
    {:via, Registry, {Craft.Registry, {name, component}}}
  end

  def lookup(name, component) do
    case Registry.lookup(Craft.Registry, {name, component}) do
      [{pid, _meta}] ->
        pid

      _ ->
        nil
    end
  end

  if Mix.env() in [:dev, :test] do
    defp set_nexus_logger do
      Logger.add_handlers(:craft)
    end

    defp silence_sasl_logger do
      Logger.add_translator({Craft.SASLLoggerTranslator, :translate})
    end
  else
    defp set_nexus_logger, do: :noop
    defp silence_sasl_logger, do: :noop
  end
end
