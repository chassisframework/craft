defmodule Mix.Tasks.Compile.Nif do
  def run(_args) do
    app = Mix.Project.config()
    build_dir = Mix.Project.app_path(app)
    case System.cmd("make", [], env: [{"BUILD_DIR", build_dir}], stderr_to_stdout: true) do
      {_result, 0} ->
        :ok

      {result, _error} ->
        IO.binwrite(result)
    end
  end
end

defmodule Craft.MixProject do
  use Mix.Project

  def project do
    [
      app: :craft,
      version: "0.1.0",
      elixir: "~> 1.13",
      compilers: [:nif] ++ Mix.compilers,
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :crypto],
      mod: {Craft.Application, []}
    ]
  end

  defp deps do
    [
      {:rocksdb, git: "git@github.com:chassisframework/erlang-rocksdb.git"}
    ]
  end

  defp elixirc_paths(:dev), do: ["lib", "test/support"]
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
