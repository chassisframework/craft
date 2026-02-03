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

# naive congestion control
config :craft, :maximum_entries_per_heartbeat, 1_000

# max log length before a compaction snapshot is triggered
config :craft, :maximum_log_length, 1_000_000

heartbeat_interval = 30 # ms
config :craft, :heartbeat_interval, heartbeat_interval

# max time in the past within which leader must have a successful quorum, or it'll step down
config :craft, :checkquorum_interval, heartbeat_interval * 10

# duration a follower will wait for a heartbeat before becoming "lonely", and starting an election
lonely_timeout = heartbeat_interval * 10
config :craft, :lonely_timeout, lonely_timeout

# setting the leader lease length to something a bit less than the lonely timeout ensures that the new leader
# can pick up the lease immediately after it's elected. this is probably what you want, it increses availability
# in split-brain scenarios (this is what TiKV does).
#
# you don't have to do this, you're free to set the value however you like, the new leader will just wait out the old lease.
config :craft, :leader_lease_period, lonely_timeout - 200

# amount of time a canddiate will wait for votes before concluding that the election has failed
config :craft, :election_timeout, 1500

# lonely nodes jitter within this window to ensure that they don't all become candidates at the same time when leadership is lost
config :craft, :election_timeout_jitter, 1500

# amount of time to wait for new leader to take over before concluding that leadership transfer has failed
config :craft, :leadership_transfer_timeout, 3000

if config_env() in [:test, :dev] do
  config :craft, :base_data_dir, "data"
else
  config :craft, :data_dir, "data"
end

if config_env() == :test do
  config :craft, :logger, [{:handler, :nexus_handler, Craft.Nexus, %{}}]
end

#import_config "#{config_env()}.exs"
