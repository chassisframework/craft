defmodule Craft.TestCluster do
  @moduledoc false

  def spawn_nodes(num) do
    case Node.start(:primary, :shortnames) do
      {:ok, _} ->
        :ok

      {:error, {:already_started, _}} ->
        :ok
    end

    1..num |> Enum.reduce([], fn _, nodes -> [spawn_node() | nodes] end) |> Enum.reverse()
  end

  defp spawn_node do
    {:ok, _pid, node} = :peer.start_link(%{name: :peer.random_name()})

    :rpc.call(node, :code, :add_paths, [:code.get_path()])
    transfer_config(node)
    start_apps(node)

    # only handle the nexus logs on the test node, otherwise we get dupes
    :rpc.call(node, :logger, :remove_handler, [:nexus_handler])

    node
  end

  defp transfer_config(node) do
    Application.loaded_applications()
    |> Enum.map(fn {app_name, _, _} -> app_name end)
    |> Enum.map(fn app_name -> {app_name, Application.get_all_env(app_name)} end)
    |> Enum.each(fn {app_name, env} ->
      Enum.each(env, fn {key, val} ->
        :ok = :rpc.call(node, Application, :put_env, [app_name, key, val, [persistent: true]])
      end)
    end)

    # :peer's remote nodes don't support ExUnit's :capture_log from nonode@nohost
    # this configures Logger in remote nodes to be enough close in behavior to :capture_log,
    # or if not configured, it uses Logger.level as fallback
    logger_level =
      case ExUnit.configuration()[:capture_log] do
        [level: level] ->
          case level do
            :emergency -> :none
            :alert -> :emergency
            :critical -> :alert
            :error -> :critical
            :warning -> :error
            :notice -> :warning
            :info -> :notice
            :debug -> :info
            _ -> :debug
          end

        true ->
          :none

        _ ->
          Logger.level()
      end

    :rpc.call(node, Application, :put_env, [:logger, :level, logger_level, [persistent: true]])
  end

  defp start_apps(node) do
    :rpc.call(node, Application, :ensure_all_started, [:mix])
    :rpc.call(node, Mix, :env, [Mix.env()])
    :rpc.call(node, Application, :ensure_all_started, [:craft], 20_000)
  end
end
