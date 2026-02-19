defmodule Craft.Bootstrap do
  @moduledoc false

  def start_dev_cluster(num \\ 5) do
    {name, nodes} =
      num
      |> Craft.TestCluster.spawn_nodes()
      |> start_dev_consensus_group()

    Craft.MemberCache.discover(name, nodes)

    name
  end

  def start_tmux_cluster do
    nodes = for i <- 2..5, do: :"#{i}@127.0.0.1"
    name = "abc"

    Craft.start_group(name, nodes, Craft.RocksDBMachine)

    {name, nodes}
  end

  def start_dev_consensus_group(nodes) do
    name = :crypto.strong_rand_bytes(3) |> Base.encode16()

    Craft.start_group(name, nodes, Craft.RocksDBMachine, global_clock: Craft.GlobalTimestamp.FixedError)

    {name, nodes}
  end

  def start_many_dev_consensus_groups(num_groups) do
    nodes = Craft.TestCluster.spawn_nodes(3)

    1..num_groups
    |> Task.async_stream(fn _ ->
      {name, _nodes} = start_dev_consensus_group(nodes)

      Craft.MemberCache.discover(name, nodes)

      name
    end, max_concurrency: 20, timeout: 120_000)
    |> Stream.run()
  end
end
