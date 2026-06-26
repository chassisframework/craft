defmodule Craft.Log do
  use GenServer

  import Craft.Application, only: [via: 2, lookup: 2]

  require Logger

  alias Craft.Persistence

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: via(args.name, __MODULE__))
  end

  def init(args) do
    Logger.metadata(name: args.name, node: node(), nexus: args[:nexus_pid])

    if nexus_pid = args[:nexus_pid] do
      remote_group_leader = :rpc.call(node(nexus_pid), Process, :whereis, [:init])
      :logger.update_process_metadata(%{gl: remote_group_leader})
    end

    state = Persistence.new(args.name, args.persistence)

    me = self()
    spawn_link(fn ->
      Process.flag(:trap_exit, true)

      receive do
        {:EXIT, ^me, _reason} ->
          terminate(:dirty, state)

        msg ->
          Logger.warning("log cleaner-upper ignored an unknown message: #{inspect msg}")
      end
    end)

    {:ok, state}
  end

  def handle(name) do
    name
    |> lookup(__MODULE__)
    |> GenServer.call(:handle)
  end

  def handle_call(:handle, _from, state) do
    {:reply, state, state}
  end

  # try to clean up gracefully, the cleaner-upper process should catch dirty exits
  def terminate(_reason, state) do
    Persistence.close(state)
  end
end
