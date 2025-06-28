defmodule Craft.Application do
  @moduledoc false

  use Application

  @impl Application
  def start(_type, _args) do
    silence_sasl_logger()
    set_nexus_logger()

    case :net_kernel.get_state() do
      %{started: :no} ->
        ensure_disterl!()

      _ ->
        :ok
    end

    create_data_dir!()

    children = [
      Craft.SnapshotServer,
      {Task.Supervisor, name: Craft.SnapshotServer.Supervisor},
      {DynamicSupervisor, [strategy: :one_for_one, name: Craft.Supervisor]},
      {Registry, keys: :unique, name: Craft.Registry},
      Craft.MemberCache
    ]

    Supervisor.start_link(children, strategy: :rest_for_one)
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

  defp create_data_dir! do
    if Application.get_env(:craft, :base_data_dir) do
      data_dir =
        Path.join([
          Application.get_env(:craft, :base_data_dir),
          to_string(Mix.env()),
          to_string(node())
        ])

      File.mkdir_p!(data_dir)

      Application.put_env(:craft, :data_dir, data_dir)
    else
      Application.fetch_env!(:craft, :data_dir) |> File.mkdir_p!()
    end
  end

  if Mix.env() in [:dev, :test] do
    defp set_nexus_logger do
      Logger.add_handlers(:craft)
    end

    defp silence_sasl_logger do
      Logger.add_translator({Craft.SASLLoggerTranslator, :translate})
    end

    defp ensure_disterl! do
      case Node.start(:craft, :shortnames) do
        {:ok, _} -> :ok
        {:error, {:already_started, _}} -> :ok
      end
    end
  else
    defp set_nexus_logger, do: :noop
    defp silence_sasl_logger, do: :noop
    defp ensure_disterl!, do: raise("Craft requires the node to be in distributed mode.")
  end
end
