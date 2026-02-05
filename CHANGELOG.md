
## v0.3.0

- Fix: metrics for the number of connected peers (real time value)
- Automatically clean up dead peers after `maxPeerDisconnectionToleranceMs` (5 minutes by default).
- Add a debug log when a peer reconnects
- Add `amITheLeader()` to get the current leader and run cluster-wide operations that require a single actor
