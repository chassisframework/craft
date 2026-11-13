defmodule Craft.PingListener do
  use GenServer

  import Craft.Tracing, only: [telemetry: 3]

  require Logger

  alias Craft.GlobalTimestamp.NativeClock
  alias Craft.Ping

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil)
  end

  @impl true
  def init(_) do
    {:ok, nil, {:continue, :open_socket}}
  end

  @impl true
  def handle_continue(:open_socket, nil) do
    {:ok, socket} = :socket.open(:inet, :dgram, :icmp)

    socket_receive(socket)

    {:noreply, socket}
  end

  @impl true
  def handle_info({:"$socket", _socket, :select, _info}, socket) do
    socket_receive(socket)

    {:noreply, socket}
  end

  def handle_info({:"$socket", _socket, :abort, _info}, socket) do
    Logger.warning "[#{inspect __MODULE__}] received socket 'abort', ignoring."

    socket_receive(socket)

    {:noreply, socket}
  end

  def handle_info(msg, socket) do
    Logger.warning "[#{inspect __MODULE__}] unexpected message #{inspect msg}, ignoring."

    {:noreply, socket}
  end


  defp socket_receive(socket) do
    received_at = NativeClock.monotonic_raw_time()

    case :socket.recv(socket, [], :nowait) do
      # strip out ipv4 header, if present
      {:ok, << 4 :: 4, ihl :: 4, _header :: size((ihl * 32) - 8), icmp_data :: binary >>} ->
        handle_async(icmp_data, received_at)
        socket_receive(socket)

      {:ok, icmp_data} ->
        handle_async(icmp_data, received_at)
        socket_receive(socket)

      {:select, _} ->
        :noop
    end
  end

  defp handle_async(<< type, code , checksum :: 16, payload :: bitstring >> = packet, received_at) do
    Task.start_link(fn ->
      case {type, code, payload} do
        {0, 0, << _identifier :: 16, _seq_num :: 16, "$CRAFT_PING$", message :: bitstring >>} ->
          if Ping.checksum(packet) == << 0, 0 >> do
            %{n: node, t: sent_at_ns} = :erlang.binary_to_term(message)
            rtt_ms = (received_at - sent_at_ns) / 1_000_000

            telemetry([:craft, :ping], %{rtt_ms: rtt_ms}, %{to_node: node})
          else
            Logger.warning "[#{inspect __MODULE__}] incorrect checksum for echo reply, ignoring. #{inspect packet}"
          end

        {3, code, payload} ->
          if Ping.checksum(packet) == << 0, 0 >> do
            Logger.warning "[#{inspect __MODULE__}] destination unreachable, code: #{inspect code}, ignoring."
          else
            # for some reason, the endianness of the encapsulated ipv4 header that's bounced back to us in a
            # "destination unreachable" ICMP message needs to be flipped for the checksum to work.
            #
            # i suspect it might be because of a broken ICMP implementation out there, or i've just missed
            # something, please reach out if you know
            #
            case payload do
              << 0, length, next_mtu :: 16, 4 :: 4, ihl :: 4, dscp :: 6, ecn :: 2, length_lsb, length_msb, rest :: bitstring>> ->
                fixed_packet = << 3, code, checksum :: 16, 0, length, next_mtu :: 16, 4 :: 4, ihl :: 4, dscp :: 6, ecn :: 2, length_msb, length_lsb, rest :: bitstring>>

                if Ping.checksum(fixed_packet) == << 0, 0 >> do
                  Logger.warning "[#{inspect __MODULE__}] destination unreachable, code: #{inspect code}, ignoring."
                else
                  Logger.warning "[#{inspect __MODULE__}] incorrect checksum for destination unreachable packet, ignoring. #{inspect packet}"
                end

              _ ->
                Logger.warning "[#{inspect __MODULE__}] malformed packet, ignoring. #{inspect packet}"
            end
          end
        
        msg ->
          Logger.warning "[#{inspect __MODULE__}] unexpected ping reply: #{inspect msg}, ignoring."
      end
    end)
  end
end
