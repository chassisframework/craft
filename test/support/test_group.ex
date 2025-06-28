defmodule Craft.TestGroup do
  alias Craft.Consensus
  alias Craft.SimpleMachine
  alias Craft.NexusCase.Formatter

  def start_group(nodes, opts \\ %{}) do
    prepare_nodes(nodes)
    name = group_name()

    {:ok, nexus} = Craft.Nexus.start(nodes, self())
    Formatter.register(nexus, Process.get(:test_id))

    manual_start = opts[:manual_start]

    opts =
      opts
      |> Enum.into(%{})
      |> Map.merge(%{nexus_pid: nexus, manual_start: true})

    Craft.start_group(name, nodes, opts[:machine] || SimpleMachine, opts)

    if !manual_start do
      run(name, nodes)
    end

    {:ok, name, nexus}
  end

  defp group_name do
    :crypto.strong_rand_bytes(3)
    |> Base.encode16()
  end

  defp prepare_nodes(nodes) do
    for node <- nodes do
      :pong = Node.ping(node)

      {:module, Craft} = :rpc.call(node, Code, :ensure_loaded, [Craft])
    end
  end

  def run(name, nodes) do
    Task.async_stream(nodes, fn node ->
      Consensus.remote_operation(name, node, :cast, :run)
    end)
    |> Stream.run()
  end

  @doc false
  def replace_consensus_state(name, node, overrides) do
    :rpc.call(node, Craft.Application, :lookup, [name, Craft.Consensus])
    |> :sys.replace_state(fn {:waiting_to_start, data} -> {:waiting_to_start, struct(data, overrides)} end)
  end
end
