defmodule Craft.Ping do
  use GenServer

  import Craft.Application, only: [via: 2]
  import Bitwise, only: [&&&: 2, >>>: 2]
  import Craft.Tracing, only: [telemetry: 3]

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
    identifier = :erlang.phash2(self(), 2 ** 16 - 1) # 16-bit identifier

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
    case node_address(state.node) do
      {:ok, address} ->
        #
        # reset seq num on 16-bit overflow
        #
        # we don't actually use the seq num for packet matching, since the payload
        # is all we care about.
        #
        seq_num =
          if state.seq_num > 2 ** 16 - 1 do
            0
          else
            state.seq_num + 1
          end

        type = 8
        code = 0
        payload = "$CRAFT_PING$" <> encode_message(state)

        packet_for_checksum = << type, code, 0 :: 16, state.identifier :: 16, state.seq_num :: 16 >> <> payload
        packet = << type, code, checksum(packet_for_checksum) :: 16, state.identifier :: 16, state.seq_num :: 16 >> <> payload

        :socket.sendto(state.socket, packet, %{addr: address, port: 0, family: :inet})

        socket_receive(state)

        :timer.send_after(@interval, :ping)

        {:noreply, %{state | seq_num: seq_num}}

      error ->
        Logger.warning "[#{inspect __MODULE__}] couldn't look up address for '#{state.node}', got: #{inspect error}"

        :timer.send_after(@interval, :ping)

        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:"$socket", _socket, :select, _info}, state) do
    socket_receive(state)

    {:noreply, state}
  end

  def handle_info({:"$socket", _socket, :abort, _info}, state) do
    Logger.warning "[#{inspect __MODULE__}] received socket 'abort', ignoring."

    socket_receive(state)

    {:noreply, state}
  end

  def handle_info(msg, state) do
    Logger.warning "[#{inspect __MODULE__}] unexpected message #{inspect msg}, ignoring."

    {:noreply, state}
  end

  defp encode_message(state) do
    :erlang.term_to_binary(%{
      n: state.node,
      t: NativeClock.monotonic_raw_time()
    })
  end

  defp socket_receive(state) do
    received_at = NativeClock.monotonic_raw_time()

    case :socket.recv(state.socket, [], :nowait) do
      # strip out ipv4 header, if present
      {:ok, << 4 :: 4, ihl :: 4, _header :: size((ihl * 32) - 8), icmp_data :: binary >>} ->
        handle_incoming(icmp_data, received_at, state)
        socket_receive(state)

      {:ok, icmp_data} ->
        handle_incoming(icmp_data, received_at, state)
        socket_receive(state)

      {:select, _} ->
        :noop
    end
  end

  defp handle_incoming(<< 0, 0, _checksum :: 16, identifier :: 16, _seq_num :: 16, "$CRAFT_PING$", message :: bitstring >> = packet, received_at, %State{identifier: identifier}) do
    if checksum_matches?(packet) do
      %{n: node, t: sent_at_ns} = :erlang.binary_to_term(message)
      rtt_ms = (received_at - sent_at_ns) / 1_000_000

      telemetry([:craft, :ping], %{rtt_ms: rtt_ms}, %{to_node: node})
    else
      Logger.warning "[#{inspect __MODULE__}] incorrect checksum for echo reply, ignoring. #{inspect packet}"
    end
  end

  # packet isn't for this process (on macos, all icmp messages are received by every process)
  defp handle_incoming(<< 0, 0, _rest :: bitstring >>, _received_at, _state), do: :noop

  defp handle_incoming(<< 3, code, checksum :: 16, payload :: bitstring >> = packet, _received_at, _state) do
    if checksum_matches?(packet) do
      Logger.warning "[#{inspect __MODULE__}] destination unreachable, code: #{inspect code}, ignoring."
    else
      # for some reason, occasionally, the endianness of the encapsulated ipv4 header that's bounced back to us in a "destination unreachable" ICMP message needs to be flipped for the checksum to work.
      #
      # i suspect it might be because of a broken ICMP implementation out there, or i've just missed something, please reach out if you know
      #
      case payload do
        << 0, length, next_mtu :: 16, 4 :: 4, ihl :: 4, dscp :: 6, ecn :: 2, length_lsb, length_msb, rest :: bitstring>> ->
          fixed_packet = << 3, code, checksum :: 16, 0, length, next_mtu :: 16, 4 :: 4, ihl :: 4, dscp :: 6, ecn :: 2, length_msb, length_lsb, rest :: bitstring>>

          if checksum_matches?(fixed_packet) do
            Logger.warning "[#{inspect __MODULE__}] destination unreachable, code: #{inspect code}, ignoring."
          else
            Logger.warning "[#{inspect __MODULE__}] incorrect checksum for destination unreachable packet, ignoring. #{inspect packet}"
          end

        _ ->
          Logger.warning "[#{inspect __MODULE__}] malformed packet, ignoring. #{inspect packet}"
      end
    end
  end

  defp handle_incoming(packet, _received_at, _state) do
    Logger.warning "[#{inspect __MODULE__}] unexpected ping reply: #{inspect packet}, ignoring."
  end

  defp node_address(node) do
    [name, host] =
      node
      |> Atom.to_string()
      |> String.split("@")
      |> Enum.map(&:erlang.binary_to_list/1)

    :erl_epmd.address_please(name, host, :inet)
  end

  def checksum(binary, sum \\ 0)
  def checksum(<< chunk :: 16, rest :: binary >> , sum), do: checksum(rest, sum + chunk)
  def checksum(<< last_byte >>, sum), do: checksum(<< last_byte, 0 >>, sum)
  def checksum(<<>>, sum) do
    sum |> carry() |> :erlang.bnot()
  end

  # recursively add the carry bits to the 16-bit sum itself until checksum fits into the bottom 16 bits
  defp carry(num) when num > 0xFFFF, do: carry((num &&& 0xFFFF) + (num >>> 16))
  defp carry(num), do: num

  defp checksum_matches?(icmp_packet) do
    << checksum(icmp_packet) :: 16 >> == << 0, 0 >>
  end
end
