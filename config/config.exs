import Config

config :logger,
  handle_otp_reports: true,
  handle_sasl_reports: true

config :logger, :default_handler,
  level: :info

config :logger, :default_formatter,
  format: "[$level] $metadata\t| $message\n",
  metadata: [:name, :t, :term, :node]

config :craft, :snapshot_server_port, 1337

if config_env() in [:test, :dev] do
  config :craft, :base_data_dir, "data"
else
  config :craft, :data_dir, "data"
end

if config_env() == :test do
  config :craft, :logger, [{:handler, :nexus_handler, Craft.Nexus, %{}}]
end

config :craft, :clock_bound_shm_path, "/var/run/clockbound/shm0"

#import_config "#{config_env()}.exs"
