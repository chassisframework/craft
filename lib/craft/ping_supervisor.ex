# TODO: stop pings when node no longer a member of any known groups
defmodule Craft.PingSupervisor do
  @moduledoc false

  alias Craft.Ping

  def start_ping(node) when node == node(), do: :error

  def start_ping(node) do
    case DynamicSupervisor.start_child(__MODULE__, {Ping, node}) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
    end
  end
end
