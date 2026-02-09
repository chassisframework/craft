# TODO: move to a centralized snapshot downloader for multi-raft coordination
defmodule Craft.SnapshotServerClient do
  @moduledoc false
  use GenServer

  alias Craft.SnapshotServer.RemoteFile

  defmodule State do
    defstruct [
      :snapshot_transfer,
      :remaining_files,
      :local_path,
      :finished_fun,
      :socket,
      :current_download
    ]
  end


  def start_link(snapshot_transfer, local_path, finished_fun) do
    args = %{
      snapshot_transfer: snapshot_transfer,
      local_path: local_path,
      finished_fun: finished_fun
    }

    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(args) do
    state =
      %State{
        snapshot_transfer: args.snapshot_transfer,
        remaining_files: args.snapshot_transfer.files,
        local_path: args.local_path,
        finished_fun: args.finished_fun
      }

    {:ok, state, {:continue, :connect}}
  end

  def handle_continue(:connect, state) do
    {:ok, socket} = :gen_tcp.connect(state.snapshot_transfer.ip, state.snapshot_transfer.port, [:binary, packet: :raw, active: false, nodelay: true])

    GenServer.cast(self(), :download_next)

    {:noreply, %{state | socket: socket}}
  end

  def handle_cast(:download_next, %State{remaining_files: []} = state) do
    state.finished_fun.(:ok)

    {:stop, :normal, state}
  end

  def handle_cast(:download_next, state) do
    [file | rest] = state.remaining_files

    {:ok, pid} = Task.start_link(__MODULE__, :download, [file, self(), state])

    {:noreply, %{state | current_download: {pid, file}, remaining_files: rest}}
  end

  def handle_cast({:error, _} = error, state) do
    state.finished_fun.(error)

    {:stop, :normal, state}
  end

  def download(%RemoteFile{} = file, caller, state) do
    :ok = :gen_tcp.send(state.socket, Path.join(state.snapshot_transfer.remote_path, file.name))

    local_file_dir = Path.join(state.local_path, Path.dirname(file.name))

    File.mkdir_p!(local_file_dir)

    result =
      local_file_dir
      |> Path.join(Path.basename(file.name))
      |> File.open([:write, :raw], fn handle ->
        do_download(state.socket, handle, file.byte_size, :erlang.md5_init())
      end)

    case result do
      {:ok, {:ok, md5}} when md5 == file.md5 ->
        GenServer.cast(caller, :download_next)

      {:ok, {:ok, _md5}} ->
        GenServer.cast(caller, {:error, :bad_checksum})

      {:ok, {:error, _} = error} ->
        File.rm(file.name)

        GenServer.cast(caller, error)

      error ->
        GenServer.cast(caller, error)
    end
  end

  defp do_download(_socket, _file, remaining_bytes, _md5) when remaining_bytes < 0, do: {:error, :unexpected_data}

  defp do_download(_socket, _file, 0, md5), do: {:ok, :erlang.md5_final(md5)}

  defp do_download(socket, file, remaining_bytes, md5) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, chunk} ->
        md5 = :erlang.md5_update(md5, chunk)
        :ok = IO.binwrite(file, chunk)

        do_download(socket, file, remaining_bytes - byte_size(chunk), md5)

      error ->
        error
    end
  end

end
