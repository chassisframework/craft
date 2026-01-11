defmodule Craft.NexusCase do
  @moduledoc """
  Detects test failures and writes the nexus event log/visualization to disk for analysis
  """
  # async: false, so that log capture doesn't capture logs from other concurrent tests, may be able to remove this
  use ExUnit.CaseTemplate, async: false

  alias Craft.TestCluster
  alias Craft.GlobalTimestamp.FixedError

  using do
    quote do
      @moduletag capture_log: true

      ExUnit.Case.register_attribute(__MODULE__, :test_id)

      import unquote(__MODULE__)
    end
  end

  setup_all do
    [nodes: TestCluster.spawn_nodes(5)]
  end

  # this allows the association of test_id -> nexus without passing the test_id around
  setup %{registered: %{test_id: test_id}} do
    Process.put(:test_id, test_id)

    :ok
  end

  setup %{nodes: nodes} = ctx do
    if ctx[:unmanaged] do
      :ok
    else
      opts =
        if ctx[:leader_leases] do
          %{global_clock: FixedError}
        else
          %{}
        end

      {:ok, name, nexus} = Craft.TestGroup.start_group(nodes, opts)
      Craft.MemberCache.discover(name, nodes)

      on_exit(fn ->
        Craft.stop_group(name)
      end)

      [name: name, nexus: nexus]
    end
  end

  defmacro nexus_test(message, var \\ quote(do: _), do: block) do
    quote do
      @test_id :erlang.unique_integer()
      test(unquote(message), context = unquote(var)) do
        import Craft.Nexus, only: [wait_until: 2, nemesis: 2, nemesis_and_wait_until: 3]

        unquote(block)
      end
    end
  end

  defmodule Formatter do
    use GenServer

    def register(nexus, test_id) do
      GenServer.call(__MODULE__, {:register, test_id, nexus})
    end

    @impl true
    def init(_opts) do
      Process.register(self(), __MODULE__)

      {:ok, %{}}
    end

    @impl true
    def handle_call({:register, test_id, nexus}, _from, state) do
      {:reply, :ok, Map.put(state, test_id, nexus)}
    end

    @impl true
    def handle_cast({:test_finished, %ExUnit.Test{state: {:failed, _}} = test}, state) do
      if test.tags.registered[:test_id] do
        visualize(state, test)
      end

      {:noreply, state}
    end

    @impl true
    def handle_cast({:test_finished, %ExUnit.Test{state: {:excluded, _}}}, state), do: {:noreply, state}

    def handle_cast({:test_finished, %ExUnit.Test{tags: %{visualize: true}} = test}, state) do
      if test.tags.registered[:test_id] do
        visualize(state, test)
      end

      {:noreply, state}
    end

    @impl true
    def handle_cast(_event, state), do: {:noreply, state}

    defp visualize(state, test) do
      {:ok, nexus_state} =
        state
        |> Map.fetch!(test.tags.registered.test_id)
        |> Craft.Nexus.return_state_and_stop()

      log = Enum.sort_by(nexus_state.log, & &1.meta.t)

      if log != [] do
        log_dir = "visualizations"

        File.mkdir_p!(log_dir)

        filename =
          ["craft_visualization", inspect(test.case), test.name, DateTime.to_unix(DateTime.utc_now()), "html"]
          |> Enum.map(&to_string/1)
          |> Enum.map(&String.replace(&1, ~r/[^\w\.]/, "_"))
          |> Enum.join(".")

        path = Path.join([log_dir, filename])

        Craft.Visualizer.to_file(log, path)
      end
    end
  end
end
