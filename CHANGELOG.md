## v0.5.0

- Fix: a PING announcing the next sequence id no longer marks the last (possibly lost) patch as received. Only real patches advance the guaranteed contiguous sequence id.
- Fix: missing patches at the end of a peer's sequence (announced by a PING but never received) are now requested. Previously the peer was marked as synced and these patches were lost.
- A peer starting with an empty database now requests the patches still retained by other peers instead of being marked as synced immediately.
- A new or reconnected peer no longer triggers a persistent ping (a stored fake patch) on all peers. Non-persistent pings are now enough to detect missing patches. The hourly persistent ping is unchanged.
- Fix: `keep_last` ignored non-NULL values when the first row read was a NULL with a higher priority. The merged row could differ between peers depending on the order patches were received (e.g. two patches with the same timestamp). NULL values (column not set by a patch) are now always ignored.
- Remove unused and incorrect `keep_last_window` SQLite window function.
- Fix: local patches always get a higher HLC timestamp than the previous local patch, even if the system clock goes backwards (NTP adjustment) or the node restarts with a clock behind its last stored patch. Previously, the new write could lose against an older write of the same peer.
- The "clock drift" warning is now displayed only once when the drift between peers exceeds 1 second, with the measured drift. It no longer floods logs when many patches are generated at the same millisecond.
- Fix: a patch stored in the database is now always applied, even if the process is killed. Received patches are saved and applied in the same transaction, in the next event loop. `upsert` saves and applies the patch in the same transaction.
- Fix: an error when applying a received patch (e.g. a CHECK constraint) no longer crashes the process. The invalid patch is not stored and will be requested again. Other patches are not blocked.
- Fix: a failed `upsert` no longer consumes a sequence id, which created a gap that other peers requested forever.
- `readYourWrite` now waits until the patch is applied, not only stored.
- Performance: patches received in the same event loop are saved and applied in a single transaction. Receiving patches is 5 times faster with 100 patches per event loop (~93,000 → ~450,000 patches/s) and up to 7 times faster during a synchronization burst (~98,000 → ~650,000 patches/s). `upsert` and isolated patches are unchanged (~36,000 and ~30,000 patches/s).
- Add performance tests in `test/test.perf.js`, executed with the other tests (set `REPLIC_SQLITE_LIB` to compare with another version).
- Fix: the heartbeat is started at the end of `migrate()` instead of the constructor, so it no longer crashes the process when it runs before `migrate()`.
- Fix: errors in the heartbeat are caught and logged instead of crashing the process.
- Fix: `upsert` without callback no longer throws `callback is not a function` on error paths; the error is logged instead.
- Fix: `exit()` waits the replication only for connected peers. Disconnected peers kept during `maxPeerDisconnectionToleranceMs` caused a timeout during rolling updates.
- Fix: the leader waits the replication before the shutdown backup. The backup is still done after a replication timeout, and the error is returned.
- Fix: the heartbeat keeps running while `exit()` waits the replication, so peers can still detect and request my last patches.
- Fix: received patches older than the retention period (`maxPatchRetentionMs`) are ignored. They could overwrite a more recent value whose patches were already deleted. They are still considered as received, so they are not requested again.


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

