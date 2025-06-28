defmodule Craft.Visualizer do
  require EEx

  @template Path.join([__DIR__, "visualizer", "index.html.eex"])

  EEx.function_from_file(:def, :render, @template, [:assigns])

  def to_file(log_lines) do
    File.write!("index.html", to_html(log_lines))
  end

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

    leadership_events = Enum.filter(events, fn event -> event.meta.trace == {:became, :leader} end)

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
        periods =
          events
          |> role_periods()
          |> Enum.map(fn
            {role, start, nil} ->
              {role, start, end_time}

            period ->
              period
          end)

        {node, periods}
      end)

    terms =
      leadership_events
      |> Enum.reduce({[], nil}, fn
        event, {acc, nil} ->
          {acc, event}

        event, {acc, last_event} ->
          term = %{term: last_event.meta.term, started_at: last_event.meta.t, ended_at: event.meta.t}
          {[term | acc], event}
      end)
      |> elem(0)

    last_leadership_event = List.last(leadership_events)
    last_term = %{term: last_leadership_event.meta.term, started_at: last_leadership_event.meta.t, ended_at: end_time}
    terms = terms ++ [last_term]

    render(events: events, nodes: nodes, terms: terms, role_periods: role_periods)
  end

  defp role_periods(events, periods \\ [], current_event \\ nil)

  defp role_periods([], periods, last_event) do
    {:became, last_role} = last_event.meta.trace
    start = last_event.meta.t

    [{last_role, start, nil} | periods]
  end

  defp role_periods([next_event | rest], [], nil) do
    role_periods(rest, [], next_event)
  end

  defp role_periods([next_event | rest], periods, current_event) do
    {:became, current_role} = current_event.meta.trace
    start = current_event.meta.t
    stop = next_event.meta.t

    role_periods(rest, [{current_role, start, stop} | periods], next_event)
  end
end
