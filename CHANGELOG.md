## v0.5.0

- Fix: a PING announcing the next sequence id no longer marks the last (possibly lost) patch as received. Only real patches advance the guaranteed contiguous sequence id.
- Fix: missing patches at the end of a peer's sequence (announced by a PING but never received) are now requested. Previously the peer was marked as synced and these patches were lost.
- A peer starting with an empty database now requests the patches still retained by other peers instead of being marked as synced immediately.
- A new or reconnected peer no longer triggers a persistent ping (a stored fake patch) on all peers. Non-persistent pings are now enough to detect missing patches. The hourly persistent ping is unchanged.
- Fix: `keep_last` ignored non-NULL values when the first row read was a NULL with a higher priority. The merged row could differ between peers depending on the order patches were received (e.g. two patches with the same timestamp). NULL values (column not set by a patch) are now always ignored.
- Remove unused and incorrect `keep_last_window` SQLite window function.

## v0.4.1

- Upgrade `better-sqlite3` and all dev deps

## v0.4.0

- Upgrade `better-sqlite3`
- Add prebuilds for Node.js 24

## v0.3.3

- Forward binary WebSocket messages to `onUnknownMessage(msg, true)` instead of trying to parse them as replication messages.
- `onUnknownMessage` now receives a second argument, `isBinary`: `true` for binary socket payloads, `false` for parsed messages with an unknown `type`.

## v0.3.2

- Automatically force `peerId` to be a number when calling `addRemotePeer(peerId)` and `closeRemotePeer(peerId)` to fix startup synchronization detection.
- Do not return an error when calling the `databaseBackupAbsolutePathFn` callback if no backup filename is provided.

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

