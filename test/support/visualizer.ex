# defmodule Craft.Visualizer do
#   require EEx

#   # alias Craft.Linearizability.Operation

#   @template Path.join([__DIR__, "visualizer", "index.html.eex"])

#   EEx.function_from_file(:def, :render, @template, [:assigns])

#   def to_file(log_lines) do
#     File.write!("index.html", to_html(log_lines))
#   end

#   def to_html(log_lines) do
#     events =
#       log_lines
#       |> Enum.sort_by(fn {occurred_at, _event} -> occurred_at end, fn o1, o2 -> DateTime.compare(o1, o2) == :lt end)
#       |> Enum.flat_map(fn
#         {occurred_at, {:trace, node, new_state, :enter, old_state, consensus_state}} ->
#           [%{occurred_at: occurred_at, node: node, type: :new_state, old_state: old_state, new_state: new_state, consensus_state: consensus_state}]

#         {occurred_at, {:trace, node, :lonely, :state_timeout, :begin_pre_vote, _consensus_state}} ->
#           [%{occurred_at: occurred_at, node: node, type: :begin_pre_vote}]

#         # handle message
#         {occurred_at, {:trace, node, state, :cast, message, _consensus_state}} ->
#           []

#         # message sent
#         {occurred_at, {:cast, from_node, to_node, message}} ->
#           []

#         {occurred_at, {:trace, node, :leader, :state_timeout, :heartbeat, _consensus_state}} ->
#           [%{occurred_at: occurred_at, node: node, type: :heartbeat}]

#         {occurred_at, {:trace, node, _state, {:timeout, :check_quorum}, :check_quorum, _consensus_state}} ->
#           [%{occurred_at: occurred_at, node: node, type: :check_quorum}]

#         x ->
#           IO.inspect x
#           []
#       end)

#     nodes =
#       events
#       |> Enum.map(&Map.fetch!(&1, :node))
#       |> Enum.uniq()

#     leadership_events = Enum.filter(events, fn event -> event.type == :new_state && event.new_state == :leader end)

#     terms =
#       leadership_events
#       |> Enum.reduce({[], nil}, fn
#         event, {acc, nil} ->
#           {acc, event}

#         event, {acc, last_event} ->
#           term = %{term: last_event.consensus_state.current_term, started_at: last_event.occurred_at, ended_at: event.occurred_at}
#           {[term | acc], event}
#       end)
#       |> elem(0)

#     last_leadership_event = List.last(leadership_events)
#     last_term = %{term: last_leadership_event.consensus_state.current_term, started_at: last_leadership_event.occurred_at, ended_at: DateTime.add(List.last(events).occurred_at, 20, :second)}
#     terms = terms ++ [last_term]

#     render(events: events, nodes: nodes, terms: terms)
#   end
# end
