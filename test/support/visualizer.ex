defmodule Craft.Visualizer do
  @moduledoc false
  require EEx

  @template Path.join([__DIR__, "visualizer", "index.html.eex"])

  EEx.function_from_file(:def, :render, @template, [:assigns])

  def to_file(log_lines, path) do
    File.write!(path, to_html(log_lines))
  end

  def message_type(msg) do
    (Module.split(msg.__struct__) -- ["Craft", "Message"]) |> Enum.join(".")
  end

  def format_struct(msg) do
    msg
    |> inspect()
    |> String.replace("{", "{<br />&nbsp;&nbsp;")
    |> String.replace(", ", ", <br />&nbsp;&nbsp;")
    |> String.replace(~r/}$/, "<br />}")
  end

  def group_id({module, nil}, node), do: [module, node] |> Enum.map(&inspect/1) |> Enum.join("_")
  def group_id({module, actor}, node), do: [module, actor, node] |> Enum.map(&inspect/1) |> Enum.join("_")
  def group_id(%{meta: %{node: node, mfa: {module, _, _}} = meta}), do: group_id({module, meta[:actor]}, node)

  def to_html(events) do
    events =
      events
      |> Enum.filter(& &1.meta[:trace])
      |> Enum.sort_by(& &1.meta.t)
      |> Enum.map(&put_in(&1.meta.t, DateTime.from_unix!(&1.meta.t, :nanosecond)))

    end_time = DateTime.add(List.last(events).meta.t, 1, :second)

    nodes =
      events
      |> Enum.map(& &1.meta.node)
      |> Enum.uniq()

    actors =
      events
      |> Enum.group_by(& &1.meta.node)
      |> Map.new(fn {node, events} ->
        actors =
          events
          |> Enum.map(fn %{meta: %{mfa: {module, _, _}} = meta} ->
            {module, meta[:actor]}
          end)
          |> Enum.uniq()

        {node, actors}
      end)

    role_periods =
      events
      |> Enum.reduce(Map.new(), fn
        %{meta: %{trace: {:became, _role}, node: node}} = event, acc ->
          acc
          |> Map.put_new(node, [])
          |> Map.update!(node, fn events -> events ++ [event] end)

        _, acc ->
          acc
      end)
      |> Map.new(fn {node, events} ->
        {node, role_periods(events)}
      end)

    render(events: events, nodes: nodes, actors: actors, role_periods: role_periods, end_time: end_time)
  end

  defp role_periods(events, periods \\ [], current_event \\ nil)

  defp role_periods([], periods, last_event) do
    {:became, last_role} = last_event.meta.trace
    start = last_event.meta.t

    [{last_role, last_event.meta.term, start, nil} | periods]
  end

  defp role_periods([next_event | rest], [], nil) do
    role_periods(rest, [], next_event)
  end

  defp role_periods([next_event | rest], periods, current_event) do
    {:became, current_role} = current_event.meta.trace
    start = current_event.meta.t
    stop = next_event.meta.t

    role_periods(rest, [{current_role, current_event.meta.term, start, stop} | periods], next_event)
  end
end
