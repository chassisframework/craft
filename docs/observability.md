# Craft Observability

## Contents

- [All Metrics](#all-metrics)
- [System Health Indicated by Metrics](#system-health-indicated-by-metrics)

## All Metrics

This section provides a general description of each telemetry
event. Some of these events are controlled or influenced by certain
timeout thresholds or explicit quantities set with Craft. Many of
these facets (such as heartbeat intervals and more) are
configurable. See
[config.exs](https://github.com/chassisframework/craft/blob/main/config/config.exs)
in the Craft library for available configurable values and their
defaults.

### Quorum

Quorum events happen through the course of the leader node
establishing quorum -- issuing heartbeats to followers and receiving
confirmatory responses from the follower nodes. Once a majority of
nodes (leader inclusive) have confirmed the heartbeat, then quorum is
established. For example, in a five-node system, if the leader sends
out a heartbeat message, it would need the response from two follower
nodes to create a majority.

Heartbeats are issued every `n` milliseconds (configurable), which is
called the *heartbeat interval*. In a system operating under normal,
healthy conditions, quorum is expected to be met once per heartbeat
interval and within the heartbeat interval.

#### `[:craft, :quorum, :heartbeat]`

Emitted by the leader node when issuing heartbeats to follower
nodes. It also measures how much time elapsed when performing the
necessary work to issue a heartbeat: bumping the write buffer,
calculating the lease, sending the heartbeat out to followers, etc.

#### `[:craft, :quorum, :succeeded]`

This metric is emitted by the leader node once it has
received confirmatory responses from the necessary number of follower
nodes to establish quorum for that round.

The quorum succeded event has two corresponding tags:

- `duration` : This is the time (in milliseconds) between when the
  heartbeat was sent by the leader to the time when a majority of
  nodes responded.
- `breathing_room` : The difference between the `heartbeat_interval`
  and the `duration`.

### Check Quorum

Mechanism the leader uses to make sure it's still (probably) a viable
leader. *Within the past `n` seconds, have I made quorum at all?* If
not, should step down. This is configured with Craft with the
[`checkquorum_interval`](https://github.com/chassisframework/craft/blob/343cff89a91449dc997e05d91eca62d09aa90da2/config/config.exs#L25-L26)
value.

- `[:craft, :check_quorum, :succeeded]`
- `[:craft, :check_quorum, :failed]`

### Persistence

`persistence` tracks the disk activity and how long it takes.
Interaction with persistence (rocksdb) includes writing the commit
buffer to disk (leader node), reading from disk (dependent node),
walking back a log (on follower nodes if instructed by leader), and
truncating logs.

#### `[:craft, :persistence, :commit_buffer]`

This times how long rocksdb takes to commit the write buffer to
disk. One buffer commit should be observed within each heartbeat
round.

The leader accumulates log messages that it needs to send to
followers. With every heartbeat, it commits all the changes from the
previous heartbeat.

Disk latency is a bottleneck, so metadata gets bundled into the write
buffer as well, including information about the latest lease.

#### Fetch and Friends

These measure the duration of general read operations and include the
following telemetry events:

- `[:craft, :persistence, :fetch]` : Tracks simple reads.
- `[:craft, :persistence, :fetch_between]` : Fetches logs between
    indices. Generally, the offset between these indices is 1000.
- `[:craft, :persistence, :fetch_from]` : This is a `fetch_between`
  call that fetches between the provided index to the latest index.

#### `[:craft, :persistence, :rewind]`

Occurs when leader determines a follower's log is incorrect and
triggers the follower to rewind to a certain point. This metric covers
timing information.

#### `[:craft, :persistence, :truncate]`

These events occur when a snapshot is triggered. At the time of a
snapshot, the state machine:

- Performs a snapshot
- Writes to a directory
- Notifies followers that the snapshot occured along with a log
    number.
- Once the above have been performed, the leader is free to clear
    up space by truncating the logs and replacing it with the snapshot
    information.

This is a slow operation. However, it is an optimization compared to
keeping logs around indefinitely. Craft will wait for idle periods to
perform snapshots.

This only triggers when the `log_length` exceeds the configured
threshold. All follower nodes must be caught up as well, since
followers will require the actual log messages to catch up to the
leader, and these would be unavailable if truncated (and thus rely on
installing a snapshot).

### Heartbeat

For every `heartbeat_interval`, heartbeat messages are sent out from the
leader node to follower nodes as part of establishing and maintaining
quorum. The leader tracks metrics for each round, going back for about
100 heartbeats. This is [configurable within Craft (with
`heartbeat_interval`)](https://github.com/chassisframework/craft/blob/343cff89a91449dc997e05d91eca62d09aa90da2/config/config.exs#L23)
and is set to a default value of`30` milliseconds at the time of
writing.

The heartbeat telemetry events are emitted by the leader when a
follower's response to a given heartbeat is received. These represent
anomalous conditions.

#### `[:craft, :heartbeat, :reply, :duplicate]`

These are emitted if ever the leader receives the same heartbeat
response after it has already received one. This should never happen,
but Craft can detect this and thus will report if it ever occurs.

#### `[:craft, :heartbeat, :reply, :missed_deadline]`

This is emitted by the leader node when a follower response arrives
*after* another heartbeat has been initiated. The current window was
missed.

- `lag_ms` : How far away from the expected time was this response received?

Laggy rounds are expected, but continually missed deadlines are a
problem. This metrics is also tagged by the follower node that is
lagging.

#### `[:craft, :heartbeat, :reply, :out_of_order]`

This occurs when a message was dropped and it indicates a disconnect
between nodes.

#### `[:craft, :heartbeat, :reply, :round_expired]`

Craft only keeps track of 100 rounds of heartbeat statistics. This
event is emitted by the leader when a heartbeat response is so far
behind that it does not exist in the tracked heartbeats.

### Role

Role events show the given state of a node with regard to
elections. The events include:

- `[:craft, :role, :lonely]` : The follower has not heard from a
  leader in a while. At time time of writing, this is configured by
  the `:lonely_timeout` configuration value for `:craft`, and has a
  [default value of 10x the
  `heartbeat_interval`](https://github.com/chassisframework/craft/blob/343cff89a91449dc997e05d91eca62d09aa90da2/config/config.exs#L28-L30)
- `[:craft, :role, :receiving_snapshot]` : The follower node is restoring its state from a snapshot.
- `[:craft, :role, :follower]`
- `[:craft, :role, :candidate]`
- `[:craft, :role, :leader]`

### Message

All message events are emitted by the sender of the message.

#### `[:craft, :message, :sent, :append_entries]`

This message is sent by the leader node. This event captures when
`append_entries` was sent -- equivalent to a heartbeat. This has
additional information on how many entries were in the
heartbeat/append, and to which node it was sent.

There is a cap set in Craft for maximum entries sent per
heartbeat. This is configurable with the
`maximum_entries_per_heartbeat` value. At the time of writing, [this
maximum is set to `1_000`
entries](https://github.com/chassisframework/craft/blob/343cff89a91449dc997e05d91eca62d09aa90da2/config/config.exs#L17).

#### `[:craft, :message, :sent, :append_entries_results]`

This message is sent by followers to the leader in response to an
`append_entries` message from the leader node. This event is a simple
increment.

#### `request_vote` and `request_vote_results`

- `[:craft, :message, :sent, :request_vote]` : Issued when a
    candidate node is asking other followers to vote for it as the new
    leader node.
- `[:craft, :message, :sent, :request_vote_results]` : Returned by
    follower nodes to the leader node.

Indicates that an election is happening.

#### `install_snapshot` and `install_snapshot_results`

- `[:craft, :message, :sent, :install_snapshot]` : emitted by the
    leader when commanding a follower to install a snapshot.
- `[:craft, :message, :sent, :install_snapshot_results]` :
    Follower-issued message that reports the success/failure of the
    attempted installation to the leader.

### Machine

Machine telemetry events are measurements of the user-defined
functionality provided to craft. How long did it take to run the
user's callbacks?

- `[:craft, :machine, :user, :handle_command]`
- `[:craft, :machine, :user, :handle_commands]`
- `[:craft, :machine, :user, :handle_query]`: This includes metadata
    on the consistency level this was run at (linearizable, eventual,
    etc.). It also includes the kind of read (e.g., follower-read,
    lease-read, quorum-read, etc.).
- `[:craft, :machine, :user, :snapshot]`

## System Health Indicated by Metrics

Below are instances where certain metrics persist and may indicate
pathologic or concerning states of the system.

### `breathing_room` Is a "Large" Number

This is a facet of the `[:craft, :quorum, :succeeded]` event. It is
emitted by leader nodes.

There is a tradeoff made between the quantity of commands performed
and committed to disk in a single heartbeat interval (efficiency)
versus sending the results back from a given command (latency).

"Breathing room" (as a concept) is the difference between the
heartbeat interval and the time spent for sending the round of
commands and receiving a minimum quorum. Breathing room is effectively
dead time spent waiting.

Therefore, `breathing_room` is a tuning metric. If there is a large
amount of breathing room, the team could possibly reduce the
heartbeat interval time and remove latency from the system with no
loss of efficiency.

### `check_quorum` Failure Metrics

This is the `[:craft, :check_quorum, :failed]` telemetry event. If
these events persist without a success for long enough (configurable),
the leader will step down.

### `fetch_between` Time Has Grown/Increased

This is the `[:craft, :persistence, :fetch_between]` event. It is
emitted by leader nodes and follower nodes.

`fetch_between` measures the time spent specifically fetching logs
from the disk *between indicies*. This difference is usually ~1000
indicies.

If the time goes up or spikes, it may be an indication of disk IO
saturation.

This is called by Craft's state machine for the logs that it
needs to apply, meaning that it can affect followers and leaders.

#### `write-optimized` Mode and Its Effect on this Metric

When write-optimized mode is enabled, the state machines will defer
writing to disk for when the system determines itself to not be
busy.

During idle times immediately following busy times, `fetch_between`
events will happen more frequently as deferred work is being
performed.

### `rewind` Metrics Are Frequently Observed

This is the `[:craft, :persistence, :rewind]` event.

This occurs when a leader determines a follower's log is incorrect,
and the follower "rewinds" or moves back in its log so that the
correct log may be applied from that point.

This should rarely, if ever, happen and should be monitored closely
when observed at all.

### `missed_deadline` Metrics Are Frequently Observed

This is the telemetry event `[:craft, :heartbeat, :reply,
:missed_deadline]`. It is emitted by the leader.

Heartbeats are triggered when a follower responds to a leader
heartbeat. `missed_deadline` metrics get recorded when the follower's
heartbeat response arrives back at a leader *after* another heartbeat
has been initiated.

While laggy rounds can occur, coninuous `missed_deadline` events
indicate deeper issues.

This metric is tagged by what follower is lagging and by how much, so
it is possible to isolate specific laggy nodes in the cluster.

### `duplicate` Metrics Are Observed at All

This is the `[:craft, :heartbeat, :reply, :duplicate]` event. It is
emitted by the leader. This should never happen.

This one occurs when the leader receives the same heartbeat response
after it has already been received.

### `out_of_order`

This is the `[:craft, :heartbeat, :reply, :out_of_order]` event. It
is emitted by the leader.

These occur when a message was dropped. It may indicate a disconnect
between nodes.

### `round_expired` Metrics Are Observed at All

This is the `[:craft, :heartbeat, :reply, :round_expired]` event. It
is emitted by the leader node.

This is a more magnified version of `missed_deadline` above. Craft
stores the heartbeat statistics of the past ~100
rounds. `round_expired` surfaces when a heartbeat response is so far
behind that the leader has no memory of the round it relates to.

### `lonely` Metric Is Observed with High Frequency

This is the `[:craft, :role, :lonely]` telemetry event. This is
emitted by a follower node. It will be emitted when the follower has
not heard from a leader node in a while. We are likely to see these
metrics tick up with `check_quorum` failure events.

### `receiving_snapshot` Metric Is Observed at All

This is the `[:craft, :role, :receiving_snapshot]` telemetry event.

This is emitted by a follower that assumes the `receiving_snapshot`
role. This indicates that the follower's log is so far behind that the
leader will send a snapshot rather than send them their missing log
entries. This should not happen under ordinary operations and may
indicate serious network issues. This should probably alert any time it is
observed.

The only exception to this case is in the instance where the cluster
is being scaled-up and new nodes are being added to the system. New
nodes would lack any log state and would thus need to receive a
snapshot to get to the desired state.

### `request_vote` and `vote_results` Events Are Observed with High Frequency

These are the `[:craft, :message, :sent, :request_vote]` and `[:craft,
:message, :sent, :request_vote_results]` telemetry events. These are
observed during elections. While elections are a normal part of Raft
operations, frequent elections may indicate a failure to make quorum
(and thus observe `check_quorum.failure` and `role.lonely` metrics
tick up as well).

We can expect to see an increase in elections during:

- Network segmentation
- Resource overload (CPU or other)
- Deploys

### Any Machine Metrics Moving Up in Time

The `[:craft, :machine, :user, _]` class of telemetry events are
measurements of callbacks implemented by clients of Craft. If we see
times jump up in any of these metrics, it indicates something
happening in the Authz machine implementation that uses Craft.
