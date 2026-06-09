defmodule Craft.Linearizability.Visualization do
  require EEx

  # alias Craft.Linearizability.Operation

  @template Path.join([__DIR__, "visualization", "index.html.eex"])

  EEx.function_from_file(:def, :render, @template, [:assigns])

  def to_file(_linearized_operations) do
    {linearized_operations, ignored_operations} = File.read!("/Users/mikes/repos/craft/history") |> :erlang.binary_to_term()#|> Enum.take(20)

    operations =
      List.flatten(
        Enum.map(linearized_operations, fn op -> Map.put(op, :linearized?, true) end),
        Enum.map(ignored_operations, fn op -> Map.put(op, :linearized?, false) end)
      )

    File.write!("index.html", to_html(operations))
  end

  def to_html(operations) do
    minimum_called_at = Enum.min_by(operations, & &1.called_at).called_at
    operations = Enum.map(operations, fn op ->
      %{op | called_at: op.called_at - minimum_called_at, received_at: op.received_at - minimum_called_at}
    end)

    shortest_duration =
      operations
      |> Enum.map(fn op -> op.received_at - op.called_at end)
      |> Enum.min()

    width_per_unit_time = 10 / shortest_duration
    operations = Enum.map(operations, fn op ->
      %{op | called_at: op.called_at * width_per_unit_time, received_at: op.received_at * width_per_unit_time}
    end)

    operations_by_client =
      operations
      |> Enum.group_by(& &1.client)
      |> Map.new(fn {client, ops} ->
        ops = Enum.sort_by(ops, & &1.called_at)

        {client, ops}
      end)

    linearized_ids =
      operations
      |> Enum.filter(& &1.linearized?)
      |> Enum.map(&inspect(&1.id))

    render(operations_by_client: Enum.with_index(operations_by_client), linearized_ids: linearized_ids)
  end
end
