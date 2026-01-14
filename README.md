# Craft

todo:
- multi-raft
- cluster splitting
- smart appendentries intervals for commands (immediate, interval-based batching...)
- speculative batch-writing for speed? (take snapshot, commit speculative, continue or revert to snapshot)
- linearizable follower reads via read-index?
- broadcast replication lag to clients, to allow them to target non-linearizable reads to specific followers
- be consistent with "members" vs "nodes" nomenclature
- adaptive heartbeat intervals
- 4.2.1 server catch up "rounds" hueristic
- deferred writes in user machine, if response to command from user machine will only be `:ok` (write-only, no stateful response from command), then craft's machine
  can delay applying writes to user's machine until a read occurs, or a quiescent period. this should avoid the blocking command delay of the user's machine, but still
  maintains correctness because the write has been confirmed by quorum. this would require the user to opt into a `write-optimized` machine option, to confirm that 
  `handle_command` will only return `:ok`
- flow control (max bytes per message, sliding window, etc)
- don't deserialize entries when fetching from persistence for transmission to followers (also lets us know how big a message is going to be, for congestion control)

features:
- leader leases for fast linearizable reads (pluggable time-source support, batteries included with clockbound)
- linearizability checker with fault injector + visualizer
- cluster visualizer
- 3.10 leadership transfer extension
- PreVote
- CheckQuorum
- rocksdb backend
- fast snapshot transfer via sendfile sysctl
