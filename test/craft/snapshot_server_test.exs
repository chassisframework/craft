defmodule Craft.SnapshotServerTest do
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Craft.SnapshotServer

  @tag :tmp_dir
  test "a client cannot read a file outside data_dir via path traversal", %{tmp_dir: tmp_dir} do
    # a data_dir with nothing secret in it
    data_dir = Path.join(tmp_dir, "data")
    File.mkdir_p!(data_dir)

    secret_path = Path.join(tmp_dir, "secret.txt")
    secret_contents = "top-secret-#{:erlang.unique_integer([:positive])}"
    File.write!(secret_path, secret_contents)

    port = start_server(data_dir)

    traversal =
      (tmp_dir
      |> Path.split()
      |> Enum.reduce(secret_path, fn _, acc -> "../" <> acc end))

    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])
    :ok = :gen_tcp.send(sock, traversal)

    assert "" == recv_all(sock, ""), "received data from a file outside the data directory"
  end

  @tag :tmp_dir
  test "a client can read a legitimate nested file inside data_dir", %{tmp_dir: tmp_dir} do
    data_dir = Path.join(tmp_dir, "data")

    relative_name = "snapshots/42/log/1.sst"
    contents = "snapshot-bytes-#{:erlang.unique_integer([:positive])}"
    file_path = Path.join(data_dir, relative_name)
    File.mkdir_p!(Path.dirname(file_path))
    File.write!(file_path, contents)

    port = start_server(data_dir)

    {:ok, sock} = :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false, packet: :raw])
    :ok = :gen_tcp.send(sock, relative_name)

    assert contents == recv_all(sock, "")
  end

  defp start_server(data_dir) do
    server =
      start_supervised!(%{
        id: SnapshotServer,
        start: {GenServer, :start_link, [SnapshotServer, %{data_dir: data_dir, port: 0}]}
      })

    %{port: port} = GenServer.call(server, :config)
    port
  end

  defp recv_all(sock, acc) do
    case :gen_tcp.recv(sock, 0, 2_000) do
      {:ok, chunk} -> recv_all(sock, acc <> chunk)
      {:error, :closed} -> acc
      {:error, :timeout} -> acc
    end
  end
end
