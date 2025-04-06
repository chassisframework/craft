defmodule Craft.ParallelClients do
  alias Craft.Linearizability.Operation

  def run(fun, num, num_commands) do
    1..num
    |> Task.async_stream(fn _ -> do_commands(fun, num_commands) end, timeout: :infinity)
    |> Enum.flat_map(fn {:ok, ops} -> ops end)
  end

  def start(fun, num) do
    loop =
      fn ->
        (fn recurse ->
          recurse.(recurse, {0, []})
        end).(fn recurse, {i, ops} ->
          receive do
            {:stop, pid} ->
              send(pid, {self(), ops})
          after
            0 ->
              recurse.(recurse, {i+1, [do_command(fun, i) | ops]})
          end
        end
        )
      end

    for _ <- 1..num do
      spawn_link(loop)
    end
  end

  def stop(pids) do
    for pid <- pids do
      send(pid, {:stop, self()})
    end

    for pid <- pids do
      receive do
        {^pid, ops} ->
          ops
      end
    end
    |> List.flatten()
  end

  defp do_commands(fun, :infinity) do
    Stream.iterate(1, & &1 + 1) |> Enum.reduce([], fn i, ops -> [do_command(fun, i) | ops] end)
  end

  defp do_commands(fun, num) do
    for i <- 1..num do
      do_command(fun, i)
    end
  end

  defp do_command(fun, i) do
    called_at = :erlang.monotonic_time()
    {command, response} = fun.(i)
    received_at = :erlang.monotonic_time()

    %Operation{
      id: {self(), called_at},
      client: self(),
      called_at: called_at,
      received_at: received_at,
      command: command,
      response: response
    }
  end
end
