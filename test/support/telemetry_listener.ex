defmodule Craft.TelemetryListener do
  def handle_event(event, measurements, meta, _config) do
    IO.inspect({event, measurements, meta}, label: node())
  end

  def attach do
    :telemetry.attach_many(
      __MODULE__,
      Craft.telemetry_events(),
      &__MODULE__.handle_event/4,
      nil
    )
  end
end
