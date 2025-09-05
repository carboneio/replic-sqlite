# Replic-sqlite

**Fast Conflict-free Replication SQLite**

Replic-sqlite is a lightweight module for Node.js/Bun.js that brings multi-writer, CRDT-based replication to SQLite without performance compromises.


## Philosophy

- 500 LOC, No hidden magic.
- Performance first: avoid costly operations whenever possible (JSON parsing, ...)
- Multi-writer and multi-reader by default.  
- Built on Conflict-free Replicated Data Types (CRDTs).  
- No single point of failure by design.  
- Eventually consistent with guaranteed convergence: replicas may differ at time T, but all nodes will converge to the same state.
- Optionally enforces "read your own write" (requires external HTTP header token or reverse proxy).  
- No limits on network protocols: WebSocket, TCP, HTTP, UDP, Pub/Sub bus.
- No limits on network topology: supports hub-and-spoke, mesh, or mixed setups.  
- Tolerant to unordered packets.  
- No external dependencies, except SQLite.  
- Built-in migration system: supports live, transparent DB migrations without locks
- Less than 800 lines of code, including the migration system
- Supports stop and restart with automatic recovery on startup
- Rock-solid, even with slow or unreliable internet connections
- Can be embedded directly into the application
- No leader election -> no bottleneck for writes 
- Only tables with their correspondant `_patches` tables are replicated (see below)

## Known Limits

- Tables are patched independently, so constraints between tables cannot be used.  
  This issue can be addressed later using a "group patch" feature.
- Direct Insert/Delete/Update request are forbidden in synchronized tables, unless you know what you do (Direct Deletes can be used to clean old data)

## Prerequisites

1) Every table in your database must have:
   - A primary key  
   - A separate table to store temporary patches, with three reserved and mandatory columns:  
     - `_patchedAt`  : 53bits number (Unix timestamp in millisecond)
     - `_sequenceId` : 64bits Consecutive sequence number of change per peer
     - `_peerId`     : 32bits globally unique, Source of change


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

