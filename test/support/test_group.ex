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
  # state modification function needs to be re-constructed on test nodes, since anon funcs in .exs files (test files) aren't shipped to remote test nodes. 
  def replace_consensus_state(name, node, quoted_fun) do
    :ok = :rpc.call(node, __MODULE__, :do_replace_state, [name, quoted_fun])
  end

  @doc false
  def do_replace_state(name, quoted_fun) do
    {fun, []} = Code.eval_quoted(quoted_fun)

    name
    |> Craft.Application.lookup(Craft.Consensus)
    |> :sys.replace_state(fn {:waiting_to_start, state} -> {:waiting_to_start, fun.(state)} end)

    :ok
  end
end
