defmodule Craft.TelemetryListener do
  require Logger

  def handle_event(event, measurements, metadata, _config) do
    IO.inspect("#{inspect event} #{inspect measurements}", label: node())
  end

  def attach do
    metrics = [
      [:craft, :heartbeat],
      [:craft, :heartbeat, :append_entries],
      [:craft, :heartbeat, :reply, :duplicate],
      [:craft, :heartbeat, :reply, :missed_deadline],
      [:craft, :heartbeat, :reply, :out_of_order],
      [:craft, :heartbeat, :reply, :round_expired],
      [:craft, :machine, :user, :snapshot],
      [:craft, :machine, :user, :handle_query],
      [:craft, :machine, :user, :handle_command],
      [:craft, :machine, :user, :handle_commands],
      [:craft, :quorum, :succeeded],
    ]

    for metric <- metrics do
      :ok = :telemetry.attach(metric, metric, &__MODULE__.handle_event/4, nil)
    end
  end
end
