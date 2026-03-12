<p align="center">
  <a href="https://www.npmjs.com/package/replic-sqlite">
    <img src="https://badgen.net/npm/dt/replic-sqlite" alt="npm badge">
  </a>
  <a href="https://www.npmjs.com/package/replic-sqlite">
    <img src="https://badgen.net/npm/dm/replic-sqlite" alt="npm badge">
  </a>
  <a href="https://www.npmjs.com/package/replic-sqlite">
    <img src="https://badgen.net/npm/v/replic-sqlite" alt="replic-sqlite version badge">
  </a><br/>
</p>

# Replic-sqlite  

> **ℹ️ Project Status:** This project is actively developed and evolving. While still in progress, it is already used in production environments.

`replic-sqlite` is a lightweight Node.js / Bun.js module that adds **multi-writer, Conflict-Free replication** to SQLite — without performance compromises.  


## ✨ Philosophy & Core Features  

- 🧩 **Tiny & Transparent** – ~800 LOC, no hidden magic.
- 🚀 **Performance First** – avoids expensive ops (like excessive JSON parsing).
- 🔀 **True Multi-Writer** – no master, no leader bottlenecks.
- 📚 **CRDT at the Core** – each row is replicated conflict-free.
- 🛡️ **No Single Point of Failure** – designed for resilience.
- 🔄 **Eventual Consistency** – replicas may diverge temporarily, but always converge.
- 👤 **Read-Your-Own-Writes (optional)** – via session token or reverse proxy.
- 🌐 **Protocol Agnostic** – WebSocket, TCP, HTTP, UDP, Pub/Sub — your choice.
- 🕸️ **Flexible Topologies** – hub-and-spoke, mesh, hybrid.
- 📦 **Dependency-Light** – only depends on SQLite itself.
- 🔧 **Rolling Upgrades** – live DB migration system for upgrades/downgrades.
- 🛸 **Alien-Compatible** – handles heterogeneous DB schemas across the cluster, making rolling upgrades transparent
- 💤 **Crash-Safe** – survives stop/restart with automatic recovery.
- 💪 **Self-Healing** – handles packet loss, reordering, clock drift.
- ⚙️ **Embedded-Friendly** – drop it directly into your app.
- 📊 **Open Metrics** – built-in observability.
- 🎯 **Selective Replication** – only tables with `_patches` tables are synced.
- 🌩️ **Serverless-Friendly** – automatic snapshot/backup/recovery to external object storage.


## 🚀 Why Replic-sqlite?

A better alternative to libSQL, Turso, Bedrockdb, cr-sqlite, dqlite, rqlite, and litestream if you use embedded SQLite with Node.js or Bun.sh.  

**Why not store SQL statements and apply them in order using a blockchain-based system (like [BedrockDB](https://bedrockdb.com/)) to guarantee consistency?**

- Storing SQL statements is inefficient.
- Schema changes are difficult to handle. For example, should a SQL statement be sent over the network if a node has not been migrated yet, or if it has already been migrated?
- This approach requires strong ordering guarantees, usually enforced by a leader, to ensure consistency. In contrast, CRDT patches can be applied in any order. This leader becomes a bottleneck.

**Why not logical replication, or WAL replication?**

- These replication mechanisms require access to low-level SQLite internals to extract changesets or patchsets. This is possible with the SQLite session extension: https://sqlite.org/sessionintro.html
- They only work when the schema is exactly the same on all nodes, which is not true during a rolling update.
- Like statement replication (see above), they require a synchronization mechanism to ensure that patches are applied in the same order.

**Why are patches stored in per-table duplicate tables instead of a global `log` table?**

- **1. Better read/write performance**  
  Storing patches in a single global table would require a generic column type (such as `BLOB` or `JSON`) to support multiple table schemas.
  Reading and writing such data requires serialization and deserialization, which adds overhead.
  The only exception is when database versions do not match. In that case, patches are temporarily stored in a global table called `pending_patches`.
  This only happens for a short time during schema upgrades.

- **2. Easier schema migrations**  
  When the schema changes, it is easier to migrate patches if they are stored in a dedicated `_patches` table for each main table.
  In practice, you can reuse the migration logic from the main table and apply the same changes to its corresponding `_patches` table.
  Without this approach, migrating existing patches would be much more complex, as it would require deserializing every patch, applying the migration, and serializing everything again.

**Why must patches be migrated like any other table?**

- When a peer starts, it begins from the latest database backup and replays the patchsets received from other peers since that backup.
- For this reason, each peer must retain a certain amount of patch history in the database so it can synchronize newly created or recently restarted peers.


## Prerequisites

1) Every table in your database must have:
   - A primary key  
   - A separate table to store temporary patches, with three reserved and mandatory columns:  
     - `_patchedAt`  : 53bits number (Hybrid Logical Clocks), timestamp since 2025-01-01 + counter
     - `_sequenceId` : 53bits Consecutive sequence number of change per peer
     - `_peerId`     : 53bits globally unique, Source of change


```sql
  /* Your table */
  CREATE TABLE myTable (
    id            INTEGER NOT NULL,
    tenantId      INTEGER NOT NULL,
    name          TEXT,
    deletedAt     INTEGER,
    
    PRIMARY KEY (id, tenantId)
  ) STRICT;

  /* YOU MUST CREATE THE CORRESPONDING TABLE IN YOUR MIGRATIONS */
  CREATE TABLE myTable_patches (
    _patchedAt    INTEGER  NOT NULL,
    _sequenceId   INTEGER  NOT NULL,
    _peerId       INTEGER  NOT NULL,
      
    id            INTEGER NOT NULL,
    tenantId      INTEGER NOT NULL,
    name          TEXT,
    deletedAt     INTEGER
  ) STRICT;
```

2) Direct DELETE is forbidden.  The nature of CRDTs requires that a deletion must be a patch with `deletedAt`.


## How it works?

Each node in the cluster has a globally unique ID, usually generated at startup.

When a data change must be applied to a database table (for example, `app.upsert("myTable", {column : 1})`), `replic-sqlite`:

- generates a patch and stores it in the `myTable_patches` table.)
- applies the patch to the actual `myTable` table using an UPSERT
- sends the patch to all other peers in the cluster

**What is a patch?**

A patch contains:

- the name of the table to update
- the delta, containing all fields of the target row as a JSON object (for example, `{ "columnA": "23", "columnB": "" }`)
- a patch sequence number, which is strictly contiguous and unique per peer
- a patch timestamp based on a hybrid logical clock (HLC)
- the `peerId` of the node that created the patch

```js
  {
    type  : MESSAGE_TYPES.PATCH,
    at    : Date.now(),          // HLC timestamp 
    peer  : myPeerId,            // source of patch
    seq   : lastSequenceId + 1,  // patch sequence
    ver   : dbVersion,           // db version of source peer
    tab   : tableName,           // table name
    delta : rowPatch
  }
```

**How does each node ensure convergence?**

- Patches are stored in a dedicated table such as `myTable_patches` and are always applied in a deterministic order: patch timestamp, then `peerId`, then sequence number.
- When a node receives a patch, it merges that patch with all patches whose timestamp is greater than or equal to the incoming patch timestamp, always using the ordering described above.
- This produces a new patch, which is then applied to the actual table.

**How does a node detect a missing patch?**

The sequence number is strictly contiguous for each peer. If there is a gap in the sequence, the node detects that one or more patches are missing and requests them again from the source node.

This is the message that is SENT to the other node to send missing patches:

```js
  {
    type    : MESSAGE_TYPES.MISSING_PATCH,
    peer    : myPeerId,              // peer
    minSeq  : missingSequence,       // min sequence of peer to get (inclusive)
    maxSeq  : missingSequence,       // last sequence to send (inclusive)
    forPeer : peerNb                 // where the patch is missing
  }
```

## API description

```js
const Database = require('better-sqlite3');
// We recommend naming the on-disk database file after the peer ID.
const db = new Database();
const ReplicSQLite = require('replic-sqlite');
const app = ReplicSQLite(db, existingPeerId, /* auto-generated if not provided */ {
    // Prefix used for logs when DEBUG=replic-sqlite:*
    debugPrefix: 'replic-sqlite',
    // Prefix used for OpenMetrics values returned by metrics()
    metricsPrefix: 'db',
    // Recommended value: 5 seconds. Every 5 seconds (with jitter), the node:
    // - sends a ping to other nodes to verify they are alive and in sync
    // - checks whether patches are missing and requests them if necessary
    // In addition, every hour (not configurable), it sends a persistent ping
    // message stored in pending_patches with tableName set to '_'.
    heartbeatIntervalMs: 5000,
    // How long patches are retained in the database (25 hours by default).
    // This value must be greater than the maximum age of the latest backup,
    // so that a peer can replay other peers' patch history since that backup.
    maxPatchRetentionMs: 1000 * 60 * 60 * 25,
    // Called when preparing statements for each table, allowing the generated
    // SQL used for reads and writes to be overridden.
    prepareStatementHook: (tableName, column) => {
      // return { write: ?, read: json(b) }
    },
    // Whether each peer socket sends raw strings or already parsed objects.
    socketStringMode: false,
    // Called when an unknown message is received.
    // Useful if you reuse the same transport protocol for application messages.
    onUnknownMessage: (msgParsed) => {}, // if valid JSON, the message is already parsed
    
    // Called when a backup is generated. 
    // Must call next() callback with absolute path where the backup should be stored.
    // Only the leader actually starts the backup.
    // If next(null), next(), or next('') is called, the backup is skipped.
    // backupType can be "scheduled" or "shutdown", depending on what triggered it.
    databaseBackupAbsolutePathFn: (backupType = 'scheduled', next) => {
      next(path.join(process.cwd(), `${backupType}_date.sqlite`));
    }
    // Backup cron schedule (system timezone)
    databaseBackupCron: '0 1 * * *',
    // When a peer retransmits missing patches to another peer, this defines
    // how many patches are sent per heartbeat interval.
    maxPatchPerRetransmission: 2000,
    // How long to keep remote peer state before removing it.
    // This helps reduce leader election flapping.
    maxPeerDisconnectionToleranceMs: 5 * 60 * 1000, // 5 minutes
  }
);

// Get OpenMetrics values
app.metrics();

// Add a remote peer.
// The socket must expose two functions: socket.send and socket.on('message', fn)
app.addRemotePeer(remotePeerId, socket);

// Remove a remote peer.
// This must be called as soon as the connection to a remote peer is lost,
// so the system can determine whether a new leader election is required after
// maxPeerDisconnectionToleranceMs.
app.closeRemotePeer(remotePeerId, socket);

// By default, a node is NOT a leader at startup.
// Leader election is evaluated only in the following cases:
// - after a call to addRemotePeer
// - after a call to closeRemotePeer + maxPeerDisconnectionToleranceMs
// - after a call to amITheLeader() if the role is still undetermined
app.amITheLeader(); // returns true or false

// Fired only once for each peer added with addRemotePeer
// (even if addRemotePeer is called multiple times for the same peer),
// when we have received all recent messages from that peer and are fully in sync.
app.event.on('synced', function (peerId) {});

// Start a new backup (backupType can be 'scheduled' or 'shutdown')
app.backupDatabase(backupType = 'shutdown', callback);

// If no callback is provided, the following events are emitted instead:
app.event.on('backup:completed', function (backupType, backupPath) {});
app.event.on('backup:failed', function (backupType, backupPath) {});

// The progress event is always emitted
app.event.on('backup:progress', function (backupType, backupPath, progressPercentage) {});

// Apply migrations to the database.
// Upgrades and downgrades are handled automatically.
//
// If a received patch does not match the current database version,
// it is stored in the pending_patches table and applied later. (TODO)
app.migrate([
  { up: '', down: '' }, // DB version = 1
  { up: '', down: '' }, // DB version = 2
]);

// Modify a table.
// tableName must refer to an existing table managed by replic-sqlite.
// patch must contain the column values to update for that table.
app.upsert(tableName, patch, callback); // callback(err, sessionToken)

// Can be used in tests to verify that a table update is valid
// (for example, to catch schema-related errors before writing).
app.selfTest(tableName, patch, callback);

// Generate an Express middleware to ensure read-your-write consistency
// when user requests may be routed to any node in the cluster.
service.use(app.readYourWrite('carbone-session-token', 500 /* timeout in ms */));

// If no callback is provided, exit returns immediately without waiting
// for backup or synchronization.
//
// On shutdown, behavior depends on whether the peer is the leader:
//
// - If the peer is the leader, and
//   databaseBackupAbsolutePathFn('shutdown') returns a backup path,
//   the system starts a backup and the callback is called once the backup
//   is complete with backupType and backupPath.
//
// - If the peer is not the leader, it waits until all peers have received
//   all patches produced by this peer.
//   During this period, no additional writes should be made to the database
//   (no call to app.upsert).
//
// - A backoff mechanism is used. If synchronization cannot be completed
//   within 5 × heartbeatIntervalMs, an error is logged and the callback
//   is called anyway.
app.exit((err, backupType, backupPath) => {});
```


## Known Limits

- Tables are patched independently, so constraints between tables cannot be used.  
  This issue can be addressed later using a "group patch" feature.
- Direct Insert/Delete/Update request are forbidden in synchronized tables, unless you know what you do


## 📅 Roadmap  

Additional documentation, examples, and usage guides are on the way. Stay tuned!  


## ❤️ Contributing  

This is an early-stage project — feedback, ideas, and contributions are very welcome!  


## 📜 License  

Apache-2.0
