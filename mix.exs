defmodule Craft.MixProject do
  use Mix.Project

  def project do
    [
      app: :craft,
      version: "0.1.0",
      elixir: "~> 1.13",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      aliases: aliases()
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
      {:rocksdb, git: "git@github.com:emqx/erlang-rocksdb.git"}
    ]
  end

  defp aliases do
    [
      "compile.nif":
        "cmd cc -fPIC -shared -dynamiclib -undefined dynamic_lookup -I /opt/homebrew/Cellar/erlang/27.3.3/lib/erlang/usr/include -o priv/clock_wrapper.so c_src/clock_wrapper.c"
    ]
  end

  defp elixirc_paths(:dev), do: ["lib", "test/support"]
  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]
end
