# Replic-sqlite  

> **⚠️ WIP Notice:** This project is still under active development. **Do NOT use in production yet.**

`replic-sqlite` is a lightweight Node.js / Bun.js module that adds **multi-writer, Conflict-Free replication** to SQLite — without performance compromises.  

---

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
- 💤 **Crash-Safe** – survives stop/restart with automatic recovery.
- 💪 **Self-Healing** – handles packet loss, reordering, clock drift.
- ⚙️ **Embedded-Friendly** – drop it directly into your app.
- 📊 **Open Metrics** – built-in observability.
- 🎯 **Selective Replication** – only tables with `_patches` tables are synced.
- 🌩️ **Serverless-Friendly** – automatic snapshot/backup/recovery to external object storage.

---

## 🚀 Why Replic-sqlite?  

A better alternative to libSQL, Turso, Bedrockdb, cr-sqlite, dqlite, rqlite, and litestream if you use embedded SQLite with Node.js or Bun.sh.  

More information coming soon.  


## Known Limits

- Tables are patched independently, so constraints between tables cannot be used.  
  This issue can be addressed later using a "group patch" feature.
- Direct Insert/Delete/Update request are forbidden in synchronized tables, unless you know what you do

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



---

## 📅 Roadmap  

Additional documentation, examples, and usage guides are on the way. Stay tuned!  

---

## ❤️ Contributing  

This is an early-stage project — feedback, ideas, and contributions are very welcome!  

---

## 📜 License  

Apache-2.0

