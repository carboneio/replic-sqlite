
## v0.3.1

- Fix cron execution by replacing `node-cron` with `croner`.

## v0.3.0

- Fixed the connected-peers metric so it now reports the real-time value.
- Automatically removes dead peers after `maxPeerDisconnectionToleranceMs` (5 minutes by default).
- Added a debug log when a peer reconnects.
- Added `amITheLeader()` to check whether the current peer is the leader, for cluster-wide operations that must run on a single node.
- Improved `app.event.on('synced', (peerId) => {})`: the event is now emitted only once per peer, after initial synchronization is complete.
- Improved leader election stability: in split-brain situations, leader changes are delayed until `maxPeerDisconnectionToleranceMs` has elapsed, reducing election flapping.
- Added automatic backups scheduled with `databaseBackupCron`.
- Added `databaseBackupAbsolutePathFn(backupType = 'scheduled', next)` to define where backups are written.
- Added support for shutdown backups.
- Added backup events:
  - `backup:completed`
  - `backup:failed`
  - `backup:progress`
- Added controlled shutdown with `app.exit((err, backupType, backupPath) => {})`:
  - leaders can trigger a shutdown backup
  - non-leaders wait until their patches have been fully replicated to other peers
  - if synchronization does not complete within `5 × heartbeatIntervalMs`, an error is logged and shutdown continues

