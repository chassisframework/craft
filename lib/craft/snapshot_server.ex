defmodule Craft.SnapshotServer do
  @moduledoc false
  use GenServer

  require Logger

  defmodule State do
    defstruct [:data_dir, :socket, :port]
  end

  defmodule SnapshotTransfer do
    defstruct [:ip, :port, :remote_path, :files]

    def new(remote_path, files) do
      {:ok, ip} =
        if ip = Application.get_env(:craft, :snapshot_server_ip) do
          :inet_parse.address(~c"#{ip}")
        else
          # ask epmd for which ip disterl is running on
          [name, host]=
            node()
            |> Atom.to_string()
            |> String.split("@")
            |> Enum.map(&:erlang.binary_to_list/1)

          :erl_epmd.address_please(name, host, :inet)
        end

      port = Craft.SnapshotServer.config().port

      %__MODULE__{ip: ip, port: port, files: files, remote_path: remote_path}
    end
  end

  defmodule RemoteFile do
    defstruct [:name, :md5, :byte_size]
  end

  def config do
    GenServer.call(__MODULE__, :config)
  end

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  @impl true
  def init(args) do
    port = args[:port] || Application.get_env(:craft, :port, 0)

    {:ok, %State{data_dir: args[:data_dir], port: port}, {:continue, :listen}}
  end

  @impl true
  def handle_continue(:listen, state) do
    {:ok, socket} = :gen_tcp.listen(state.port, [:binary, active: true, reuseaddr: true])
    {:ok, port} = :inet.port(socket)

    send(self(), :accept)

    {:noreply, %{state | socket: socket, port: port}}
  end

  @impl true
  def handle_call(:config, _from, state) do
    {:reply, %{port: state.port}, state}
  end

  # written like this so as to not block the acceptor process indefinitely, so we can respond to other messages
  @impl true
  def handle_info(:accept, state) do
    case :gen_tcp.accept(state.socket, 100) do
      {:ok, client} ->
        data_dir = state.data_dir || Application.get_env(:craft, :data_dir)
        {:ok, pid} = Task.Supervisor.start_child(__MODULE__.Supervisor, fn -> loop_receive({client, data_dir}) end)
        :ok = :gen_tcp.controlling_process(client, pid)

        handle_info(:accept, state)

      {:error, :timeout} ->
        send(self(), :accept)
    end

    {:noreply, state}
  end

  def handle_info(msg, state) do
    Logger.info("ignoring message #{inspect msg}")

    {:noreply, state}
  end

  defp loop_receive({client, data_dir} = state) do
    receive do
      {:tcp, _port, filename} ->
        filename = String.trim_trailing(filename, "\n")
        path = Path.join(data_dir, filename)

        if File.exists?(path) do
          {:ok, _bytes_sent} = :file.sendfile(path, client)
        else
          Logger.warning(~s|client requested non-existent file "#{filename}" from data_dir "#{data_dir}"|)

          :ok = :gen_tcp.close(client)
        end

        loop_receive(state)

      {:tcp_closed, _} ->
        :ok

      {:tcp_error, _, error} ->
        Logger.warning("TCP error, #{inspect error}")

    after 10_000 ->
      Logger.warning("client connection timed out waiting for request")
    end
  end
end
