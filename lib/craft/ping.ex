defmodule Craft.Ping do
  use GenServer

  import Craft.Application, only: [via: 2]
  import Bitwise, only: [&&&: 2, >>>: 2]

  require Logger

  alias Craft.GlobalTimestamp.NativeClock

  @interval :timer.seconds(1)

  def start_link(node) do
    GenServer.start_link(__MODULE__, node, name: via(node, __MODULE__))
  end

  defmodule State do
    defstruct [
      :node,
      :socket,
      :identifier,
      seq_num: 0,
    ]
  end

  @impl true
  def init(node) do
    identifier = << :erlang.phash2(self(), 2 ** 16 - 1) :: 16 >> # 16-bit identifier

    {:ok, %State{node: node, identifier: identifier}, {:continue, :ping}}
  end

  @impl true
  def handle_continue(:ping, state) do
    {:ok, socket} = :socket.open(:inet, :dgram, :icmp)

    send(self(), :ping)

    {:noreply, %{state | socket: socket}}
  end

  @impl true
  def handle_info(:ping, state) do
    {:ok, address} = node_address(state.node)

    # reset seq num on 16-bit overflow
    seq_num =
      if state.seq_num > 2 ** 16 - 1 do
        0
      else
        state.seq_num + 1
      end

    type = << 8 >>
    code = << 0 >>
    seq_number = << state.seq_num :: 16 >>
    payload = "$CRAFT_PING$" <> encode_message(state)

    packet_for_checksum = type <> code <> << 0 :: 16 >> <> state.identifier <> seq_number <> payload
    packet = type <> code <> checksum(packet_for_checksum) <> state.identifier <> seq_number <> payload

    state = %{state | seq_num: seq_num}
    :socket.sendto(state.socket, packet, sock_addr(address))

    :timer.send_after(@interval, :ping)

    {:noreply, state}
  end

  def handle_info(msg, state) do
    Logger.warning "[#{inspect __MODULE__}] unexpected message #{inspect msg}, ignoring."

    {:noreply, state}
  end


  defp node_address(node) do
    [name, host] =
      node
      |> Atom.to_string()
      |> String.split("@")
      |> Enum.map(&:erlang.binary_to_list/1)

    :erl_epmd.address_please(name, host, :inet)
  end

  defp sock_addr(addr) do
    %{addr: addr, port: 0, family: :inet}
  end

  def checksum(binary, sum \\ 0)

  def checksum(<< chunk :: 16, rest :: binary >> , sum), do: checksum(rest, sum + chunk)
  def checksum(<< last_byte >>, sum), do: checksum(<< last_byte, 0 >>, sum)
  def checksum(<<>>, sum) do
    << sum |> carry() |> :erlang.bnot() :: 16 >>
  end

  defp carry(num) when num > 0xFFFF, do: carry((num &&& 0xFFFF) + (num >>> 16))
  defp carry(num), do: num

  defp encode_message(state) do
    :erlang.term_to_binary(%{
      n: state.node,
      t: NativeClock.monotonic_raw_time()
    })
  end
end
