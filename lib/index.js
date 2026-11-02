const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const hlc = require('./hlc.js');
const Debugger = require('debug');
const EventEmitter = require('node:events');
const assert = require('node:assert');
const { performance } = require('node:perf_hooks');
const { Cron } = require('croner');

// Remote peer index pointers in the peerStats array for fast access (avoiding map lookup of objects, ...)
// [LAST_PATCH_AT_TIMESTAMP, LAST_SEQUENCE_ID, GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP, GUARANTEED_CONTIGUOUS_SEQUENCE_ID, LAST_MESSAGE_TIMESTAMP]
const LAST_PATCH_AT_TIMESTAMP = 0;
const LAST_SEQUENCE_ID = 1;
const GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP = 2;
const GUARANTEED_CONTIGUOUS_SEQUENCE_ID = 3;
const LAST_MESSAGE_TIMESTAMP = 4; // can be more recent than lastPatchAtTimestamp for non-persistent ping message
// Other peers send ping messages regularly with their last contiguous sequenceId known from me.
// We keep it to know other peers have received my last patches
const MY_GUARANTEED_CONTIGUOUS_SEQUENCE_ID_STORED_IN_REMOTE_PEER = 5;

const PENDING_PATCHES_TABLE_NAME = '_';
const MESSAGE_TYPES = {
  PATCH         : 10,
  PING          : 20,
  MISSING_PATCH : 30
};

/**
 * Initializes the SQLiteOnSteroid application.
 *
 * @param {import('better-sqlite3').Database} db
 * @param {number} myPeerId
 * @param {Object} options
 *
 *  @returns
 */
const SQLiteOnSteroid = (db, myPeerId = null, options) => {
  if (!myPeerId) {
    myPeerId = generatePeerId(); // TODO test myPeerId = null
  }
  if (!db) {
    throw new Error('Database instance is required');
  }
  db.loadExtension(options?.preBuiltExtensionPath ?? findPreBuiltExtensionPath());
  hlc._reset();

  const eventEmitter = new EventEmitter();
  const peerStartedNotSynced = new Set();

  // Options
  const debug = Debugger(`${options?.debugPrefix ?? 'replic-sqlite'}:${myPeerId}`);
  const debugWrite = Debugger(`${options?.debugPrefix ?? 'replic-sqlite'}:${myPeerId}:write`);
  const debugPing = Debugger(`${options?.debugPrefix ?? 'replic-sqlite'}:${myPeerId}:ping`);
  const debugRetry = Debugger(`${options?.debugPrefix ?? 'replic-sqlite'}:${myPeerId}:retry`);
  const METRICS_PREFIX         = options?.metricsPrefix ?? 'db';
  const SOCKET_STRING_MODE     = options?.socketStringMode ?? false;
  const HEARTBEAT_INTERVAL_MS  = options?.heartbeatIntervalMs ?? 0;  // Math.random() * 2000 + options?.heartbeatIntervalMs ?? (1000 * 5)); // 5-7 seconds with random jitter
  const MAX_PATCH_RETENTION_MS = options?.maxPatchRetentionMs ?? 1000 * 60 * 60 * 25;
  const PREPARE_STATEMENT_HOOK = options?.prepareStatementHook ?? (() => '?');
  const MAX_PATCH_PER_RETRANSMISSION = options?.maxPatchPerRetransmission ?? 2000;
  const MAX_PEER_DISCONNECTION_TOLERANCE_MS = options?.maxPeerDisconnectionToleranceMs ?? 1000 * 60 * 5; // prevent leader election flickering

  const DATABASE_BACKUP_ABSOLUTE_PATH_FN = options?.databaseBackupAbsolutePathFn ?? ((trigger = 'scheduled', cb) => cb(path.join(process.cwd(), `${trigger}.sqlite`)));
  const DATABASE_BACKUP_CRON = options?.databaseBackupCron ?? '';

  // Internal state for this peer
  let dbVersion = 1;
  // last SequenceId and patch timestamp of this peer
  let lastSequenceId = -1; // init and get from DB on restart
  let lastPatchAtTimestamp = -1;
  // heartbeat timestamps
  let lastDeleteOldPatchesTimestamp = 0;
  let lastDetectMissingPatchesTimestamp = 0;
  let lastPingStatTimestamp = 0;

  // peerSockets contains all connected peers' sockets
  const peerSockets = {};
  // peerStats contains all connected peers with their last sequenceId known from me.
  // A debouncer system keeps the last known state of the peer before deleting it
  const peerStats = {};
  const globalStatements = {
    // listMissingSequenceIds : list missing sequence ids of other peers (read all tables ending with _patches),
    // getLastPatchInfo       : get my latest sequence id and patchedAt timestamp stored in DB (read all tables ending with _patches),
  };
  const tableStatements = {
    // <tableName> : {
    //   cleanRow              : Function (row)           => clean row before saving (remove unknown columns),
    //   savePatch             : Function (patch)         => save patch in DB,
    //   applyPatches          : Function (fromTimestamp) => apply all patches from fromTimestamp to now on the table,
    // }
    // short (efficient) reserved table name for internal use. '_' is a shortcut to the table 'pending_patches'
    '_' : {
      // savePatch = save pending patch in pending_patches table if patchVersion is not the same as the current dbVersion
    }
  };
  // Patches received from peers, saved and applied all together in the next event loop
  let receivedPatches = [];

  // Saving and applying patches in the same transaction guarantees that a stored patch is always applied, even after a crash
  const _saveAndApplyPatches = db.transaction((patches) => {
    const _fromTimestampPerTable = {};
    for (const _patch of patches) {
      // If version mismatch, save it in pending_patches table for later processing
      const _tableName = (_patch.ver === dbVersion) ? _patch.tab : PENDING_PATCHES_TABLE_NAME;
      tableStatements[_tableName].savePatch(_patch);
      if (!(_fromTimestampPerTable[_tableName] <= _patch.at)) {
        _fromTimestampPerTable[_tableName] = _patch.at;
      }
    }
    for (const _tableName in _fromTimestampPerTable) {
      tableStatements[_tableName].applyPatches?.(_fromTimestampPerTable[_tableName]); // It does not exist for the pending_patches table
    }
  });

  // Metrics
  let nbConnectedPeers = 0; // updated before peerStats
  let nbMessagesSent = 0;
  let nbMessagesReceived = 0;
  let nbRetransmissionRequestsSent = 0;
  let nbRetransmissionRequestsReceived = 0;
  let nbMaintenanceTimeSeconds = 0;
  let nbReadYourWriteTimeouts = 0;
  let lastSuccessfulBackupTimestamp = 0;
  let amITheLeaderCached = null;

  let heartbeatInterval = null; // started by migrate()
  let backupTask = null;
  if (DATABASE_BACKUP_CRON) {
    try {
      backupTask = new Cron(DATABASE_BACKUP_CRON, { paused : true, protect : true }, backupDatabase);
    }
    catch (err) {
      throw new Error(`Invalid database backup cron syntax: ${err.message}`);
    }
  }

  /**
   * Backup the database
   *
   * Trigger can be 'scheduled', 'shutdown' (name)
   * Callback is called with (err, trigger, backupFileName) if provided
   * if the callback is not provided, it emits the 'backup:completed' and 'backup:failed' events
   *
   * @param {string} trigger   'scheduled' (default) or 'shutdown' (used in filename suffix)
   * @param {Function} callback
   */
  function backupDatabase (trigger = 'scheduled', callback) {
    if (typeof trigger !== 'string') {
      trigger = 'scheduled'; // Cron send the cron object as first argument
    }
    DATABASE_BACKUP_ABSOLUTE_PATH_FN(trigger, (_backupFileName) => {
      if (!_backupFileName) {
        debug('backup type "%s" disabled', trigger);
        callback?.(null, trigger, null);
        return;
      }
      db.backup(_backupFileName, {
        progress ({ totalPages: t, remainingPages: r }) {
          const _progress = ((t - r) / t * 100).toFixed(1);
          debug('database backup progress: %s%', _progress);
          eventEmitter.emit('backup:progress', trigger, _backupFileName, _progress);
          return 200;
        }
      }).then(() => {
        lastSuccessfulBackupTimestamp = Date.now();
        debug('database backup completed');
        if (callback) {
          return callback(null, trigger, _backupFileName);
        }
        eventEmitter.emit('backup:completed', trigger, _backupFileName);
      }).catch((err) => {
        debug('database backup failed: %s', err);
        if (callback) {
          return callback(err, trigger, _backupFileName);
        }
        eventEmitter.emit('backup:failed', trigger, _backupFileName);
      });
    });
  }

  /**
   * Generate all statements needed to merge patches for a given table.
   *
   * Called internally for each table ending with _patches except pending_patches by prepareAllStatements.
   *
   * @param {String} tableName
   * @returns {Object} { applyPatchesSQL : string, savePatchSQL : string, directUpsertSQL : string }
   */
  function _generateMergePatchesQueryPlan (tableName) {
    const _tableNamePatches = `${tableName}_patches`;
    const _tableInfo = db.pragma(`table_info('${tableName}')`);
    const _columns = [];
    const _columnsPatch = [];
    const _placeholders = [];
    const _readColumns = [];
    const _pkColumns = [];
    const _updateClauses = [];
    for (const col of _tableInfo) {
      const _colName = col.name;
      const _hook = PREPARE_STATEMENT_HOOK(tableName, _colName);
      _placeholders.push(_hook?.write ?? '?');
      _readColumns.push({ name : _colName, read : (_hook?.read ?? _colName) });
      _columns.push(_colName);
      if (col.pk > 0) {
        _pkColumns.push(_colName);
        _columnsPatch.push(_colName);
      }
      else {
        _updateClauses.push(`${_colName} = coalesce(excluded.${_colName}, ${_colName})`);
        _columnsPatch.push(`keep_last(${_colName}, _patchedAt, _peerId, _sequenceId)`);
      }
    }
    const _applyPatchesSQL = `
      INSERT INTO ${tableName} (${_columns.join(', ')})
      SELECT
        ${_columnsPatch.join(', \n        ')}
      FROM ${_tableNamePatches}
      WHERE _patchedAt >= ?
      GROUP BY ${_pkColumns.join(', ')}
      ON CONFLICT (${_pkColumns.join(', ')}) DO UPDATE SET
        ${_updateClauses.join(',\n        ')};
    `;
    const _savePatchSQL = `
      INSERT INTO ${_tableNamePatches} (_patchedAt, _sequenceId, _peerId, ${_columns.join(', ')})
      VALUES (?, ?, ?, ${_placeholders.join(', ')});
    `;
    const _savePatchSQLParamsFn = new Function('obj', `return [obj.at, obj.seq, obj.peer, obj.delta.${_columns.join(', obj.delta.')}]`);
    const _directUpsertSQL = `
      INSERT INTO ${tableName} (${_columns.join(', ')})
      VALUES (${_placeholders.join(', ')})
      ON CONFLICT (${_pkColumns.join(', ')}) DO UPDATE SET
        ${_updateClauses.join(',\n        ')};
    `;
    const _getPatchFromColumnSQL = `
      SELECT
        _sequenceId,
        json_object('type', ${MESSAGE_TYPES.PATCH}, 'at', _patchedAt, 'peer', _peerId, 'seq', _sequenceId, 'ver', ${dbVersion}, 'tab', '${tableName}', 'delta', json_object(${_readColumns.map(col => `'${col.name}', ${col.read}`).join(', ')})) as patch
      FROM ${_tableNamePatches}
      WHERE _peerId = ?
        AND _sequenceId >= ?
        AND _sequenceId <= ?
    `;
    const applyPatches     = db.prepare(_applyPatchesSQL);
    const savePatch        = db.prepare(_savePatchSQL);
    const deleteOldPatches = db.prepare(_generateDeleteOldPatchQuery(_tableNamePatches));
    tableStatements[tableName] = {
      cleanRow         : new Function('row', `return { ${_columns.map(col => `${col} : row.${col}`).join(', ')} }`),
      applyPatches     : (fromTimestamp) => applyPatches.run(fromTimestamp),
      savePatch        : (patch) => savePatch.run(_savePatchSQLParamsFn(patch)),
      deleteOldPatches : (timestamp) => deleteOldPatches.run(timestamp),
      knownColumns     : _columns
    };
    return {
      applyPatchesSQL       : _applyPatchesSQL,
      savePatchSQL          : _savePatchSQL,
      directUpsertSQL       : _directUpsertSQL,
      getPatchFromColumnSQL : _getPatchFromColumnSQL
    };
  }

  function _generateListSequenceIdsQuery (tableName) {
    return `SELECT _peerId AS peerId, _sequenceId AS sequenceId, _patchedAt AS patchedAt FROM ${tableName} WHERE _patchedAt >= ? AND _peerId <> ${parseInt(myPeerId,10)}`;
  }

  function _generateGetLastPatchInfoQuery (tableName) {
    return `SELECT MAX(_patchedAt) AS patchedAt, MAX(_sequenceId) AS sequenceId FROM ${tableName} WHERE _patchedAt >= ? AND _peerId = ${parseInt(myPeerId,10)}`;
  }

  function _generateGetPatchFromPendingTableQuery (customWhere = '') {
    return `SELECT _sequenceId, json_object('type', ${MESSAGE_TYPES.PATCH}, 'at', _patchedAt, 'peer', _peerId, 'seq', _sequenceId, 'ver', patchVersion, 'tab', tableName, 'delta', json(delta)) as patch FROM pending_patches 
            WHERE ${customWhere ? customWhere : '_peerId = ? AND _sequenceId >= ? AND _sequenceId <= ?'}`;
  }

  function _generateDeleteOldPatchQuery (tableName) {
    return `DELETE FROM ${tableName} WHERE _patchedAt < ?`;
  }

  /**
   * Prepares all statements needed to merge patches for all tables.
   * Called once just after migrations are applied.
   *
   * Stores all statements in the statements object.
   */
  function _prepareAllStatements () {
    const _listSequenceIds = [];
    const _getLastPatchInfo = [];
    const _getPatchFromColumn = [];
    tableStatements._.cleanRow = row => row; // for ping, we consider the row is already cleaned
    // generate queries plan for pending_patches table
    tableStatements._.savePatch = (() => {
      const _plan = db.prepare(`
        INSERT INTO pending_patches ( _patchedAt, _peerId, _sequenceId, patchVersion, tableName, delta   )
        VALUES                      ( ?         , ?      , ?          , ?           , ?        , jsonb(?))
      `);
      return (patch) => {
        _plan.run([patch.at, patch.peer, patch.seq, patch.ver, patch.tab, JSON.stringify(patch.delta)]);
      };
    })();
    tableStatements._.deleteOldPatches = (() => {
      const _plan = db.prepare(_generateDeleteOldPatchQuery('pending_patches'));
      return (timestamp) => _plan.run(timestamp);
    })();
    // generate queries plan for patch tables
    _listSequenceIds.push(_generateListSequenceIdsQuery('pending_patches'));
    _getLastPatchInfo.push(_generateGetLastPatchInfoQuery('pending_patches'));
    _getPatchFromColumn.push(_generateGetPatchFromPendingTableQuery());
    // Get all tables ending with _patches except pending_patches
    const _patchTables = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_patches' AND name != 'pending_patches'").all();
    // Generate merge patches query plan for each table
    for (const table of _patchTables) {
      const _tableName = table.name.slice(0, -8); // Remove '_patches' suffix
      try {
        // Check if index exists on _patchedAt column
        const _indexExists = db.prepare("SELECT 1 FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql LIKE '%_patchedAt%'").get([table.name]);
        if (!_indexExists) {
          console.warn(`Warning: Table ${table.name} is missing an index on _patchedAt column which may impact performance`);
        }
        _listSequenceIds.push(_generateListSequenceIdsQuery(table.name));
        _getLastPatchInfo.push(_generateGetLastPatchInfoQuery(table.name));
        _getPatchFromColumn.push(_generateMergePatchesQueryPlan(_tableName).getPatchFromColumnSQL);
      }
      catch (err) {
        console.error(`Failed to generate merge patches query plan for table ${_tableName}:`, err);
      }
    }
    // generate queries plan for global queries
    globalStatements.getLastPatchInfo = (() => {
      const _unionQuery = _getLastPatchInfo.join(' UNION ALL ');
      const _nbTables = _getLastPatchInfo.length;
      const _plan = db.prepare(`SELECT MAX(patchedAt) AS patchedAt, MAX(sequenceId) AS sequenceId FROM (${_unionQuery}) AS all_patches`);
      return (fromTimestamp) => {
        const _params = new Array(_nbTables).fill(fromTimestamp);
        return _plan.get(_params);
      };
    })();
    globalStatements.getPatchFromColumn = (() => {
      const _unionQuery = _getPatchFromColumn.join(' UNION ALL ');
      const _nbTables = _getPatchFromColumn.length;
      const _plan = db.prepare(`SELECT * FROM (${_unionQuery}) AS all_patches ORDER BY _sequenceId LIMIT ${MAX_PATCH_PER_RETRANSMISSION}`);
      return (peerId, minSequenceId, maxSequenceId) => {
        const _params = new Array(_nbTables*3);
        for (let i = 0; i < _nbTables; i++) {
          const offset = i * 3;
          _params[offset] = peerId;
          _params[offset + 1] = minSequenceId;
          _params[offset + 2] = maxSequenceId;
        }
        if (minSequenceId === maxSequenceId) {
          return _plan.get(_params); // faster to get a single patch than a list of patches
        }
        return _plan.all(_params);
      };
    })();
    globalStatements.listMissingSequenceIds = (() => {
      // Finds missing sequence ranges between stored patches.
      // The last stored patch of each peer is also returned (nbMissingSequenceIds IS NULL) to detect missing patches at the end of the sequence.
      const _unionQuery = _listSequenceIds.join(' UNION ALL ');
      const _nbTables = _listSequenceIds.length;
      const _plan = db.prepare(`SELECT * FROM (
                                  SELECT *, (lead(sequenceId, 1) OVER (PARTITION BY peerId ORDER BY sequenceId)) - sequenceId - 1 AS nbMissingSequenceIds FROM (${_unionQuery})
                                ) WHERE nbMissingSequenceIds > 0 OR nbMissingSequenceIds IS NULL ORDER BY peerId, sequenceId`);
      return (fromTimestamp) => {
        const _params = new Array(_nbTables).fill(fromTimestamp);
        const _result = _plan.all(_params);
        return _result;
      };
    })();
  }

  /**
   * Initializes the local peer's sequence information from persisted patch data in the database
   *
   * Queries the persistent database for the most recent patch sequence information.
   * This function is called at startup to restore the correct replication state.
   */
  function _initPeerSequence () {
    // const _oldestPatchTimestamp = hlc.getHLC(Date.now() - MAX_PATCH_RETENTION_MS); // TODO useless filter if oldest patch deleted?
    const _stat = globalStatements.getLastPatchInfo(0);
    if (_stat?.sequenceId > 0) {
      lastSequenceId = _stat.sequenceId;
      lastPatchAtTimestamp = _stat.patchedAt;
      // The local clock can be behind my last patch (clock went backwards before the restart)
      hlc.receive(_stat.patchedAt);
      return;
    }
    lastSequenceId = 0;
    lastPatchAtTimestamp = 0;
  }

  /**
   * Migrates the database to the latest schema version based on the provided application migrations.
   *
   * This function applies migration scripts (up or down) as needed to synchronize the database schema to the target version
   * as determined by the length of the `appMigrations` array. If the target version is less than the current database version,
   * down migrations are executed in reverse order. If the target version is greater than the current version, up migrations are
   * applied in order. Migration changes are transactional.
   *
   * @param {Array<{ up: string, down: string }>} appMigrations Array of migration objects describing the changes required for each version.
   * @returns {{ currentVersion: number, previousVersion: number }} Returns an object indicating the currently applied and previous schema version.
   */
  function migrate (appMigrations) {
    // Create migrations table if it doesn't exist
    db.exec(fs.readFileSync(path.join(__dirname, 'migrations', 'latest.sql'), 'utf8'));
    // Get existing migrations from DB
    const _existingMigrations = db.prepare('SELECT * FROM migrations ORDER BY id').all();
    const _lastAppliedId = _existingMigrations.length > 0 ? _existingMigrations[_existingMigrations.length - 1].id : 0;
    const _targetId = appMigrations.length;
    dbVersion = _targetId ?? 1;
    // Execute migrations in a transaction
    db.transaction(() => {
      if (_targetId < _lastAppliedId) {
        // Downgrade: execute down migrations in reverse order
        for (let i = _lastAppliedId; i > _targetId; i--) {
          const migration = _existingMigrations[i - 1];
          db.exec(migration.down);
          db.prepare('DELETE FROM migrations WHERE id = ?').run([i]);
        }
      }
      else if (_targetId > _lastAppliedId) {
        // Upgrade: execute up migrations in order
        for (let i = _lastAppliedId + 1; i <= _targetId; i++) {
          const migration = appMigrations[i - 1];
          db.exec(migration.up);
          db.prepare('INSERT INTO migrations (id, up, down) VALUES (?, ?, ?)').run([i, migration.up, migration.down]);
        }
      }
    })();
    _prepareAllStatements();
    _initPeerSequence();
    // The heartbeat needs the statements prepared above
    if (HEARTBEAT_INTERVAL_MS > 0) {
      clearInterval(heartbeatInterval); // migrate() can be called several times
      heartbeatInterval = setInterval(_heartBeat, HEARTBEAT_INTERVAL_MS);
    }

    return {
      currentVersion  : _targetId,
      previousVersion : _lastAppliedId
    };
  }

  /**
   * Broadcasts a message to all connected peer sockets.
   *
   * The message will be stringified using JSON.stringify before sending.
   *
   * @param {Object} msg - The message object to be broadcast to all peers.
   * @private
   */
  function _broadcast (msg) {
    for (const _peerId in peerSockets) {
      _sendMessageToPeer(_peerId, JSON.stringify(msg));
    }
  }

  /**
   * Detects missing patches since a given timestamp.
   *
   * This function analyzes the database for missing sequence IDs from a provided timestamp onward.
   * For each peer that has missing patches, it optionally updates that peer's statistics and
   * sends a retransmission request for the missing sequence range to the peer, provided a socket
   * connection exists. Updates statistics for synchronized peers as well.
   *
   * TODO: Add test for case where peerSockets[peerId] does not exist but peerId exists in DB with missing patches.
   * TODO: The socket may not exist, in which case, a request must be sent directly to another peer.
   *
   * @param {number} fromTimestamp The timestamp from which to search for missing patches.
   * @return {Array<Object>} Array of missing sequence id objects.
   */
  function _getMissingPatches (fromTimestamp) {
    const _rows = globalStatements.listMissingSequenceIds(fromTimestamp);
    const _missingSequenceIds = [];
    const _peerWithMissingIds = new Set();
    const _peerWithStoredPatches = new Set();
    // Rows are sorted by peerId, sequenceId. For each peer: the missing ranges between stored patches, then its last stored patch.
    for (let i = 0; i < _rows.length; i++) {
      const _row = _rows[i];
      if (_row.nbMissingSequenceIds === null) {
        // Last stored patch of this peer. A ping may have announced more recent patches which were never received
        _peerWithStoredPatches.add(_row.peerId);
        const _lastSequenceId = peerStats[_row.peerId]?.[LAST_SEQUENCE_ID] ?? 0;
        if (_lastSequenceId <= _row.sequenceId) {
          // Nothing missing at the end of the sequence. The guaranteed contiguous range of this peer is updated either by
          // its first missing range above, or by the "up-to-date peer" branch below.
          // Calling _requestMissingPatches here would send an empty range and prevent the peer from being synced.
          continue;
        }
        _row.nbMissingSequenceIds = _lastSequenceId - _row.sequenceId;
      }
      _requestMissingPatches(_row, _peerWithMissingIds);
      _missingSequenceIds.push(_row);
    }
    for (const _peerIdStr in peerStats) {
      const _peerId = parseInt(_peerIdStr, 10);
      if (_peerWithMissingIds.has(_peerId)) {
        // Guaranteed contiguous range already updated by its first missing range
        continue;
      }
      const _peerStat = peerStats[_peerIdStr];
      // No stored patch since fromTimestamp, but a ping announced more recent patches: request them from the guaranteed contiguous sequence
      if (!_peerWithStoredPatches.has(_peerId) && _peerStat[GUARANTEED_CONTIGUOUS_SEQUENCE_ID] < _peerStat[LAST_SEQUENCE_ID]) {
        const _missing = {
          peerId               : _peerId,
          sequenceId           : _peerStat[GUARANTEED_CONTIGUOUS_SEQUENCE_ID],
          patchedAt            : _peerStat[GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP],
          nbMissingSequenceIds : _peerStat[LAST_SEQUENCE_ID] - _peerStat[GUARANTEED_CONTIGUOUS_SEQUENCE_ID]
        };
        _requestMissingPatches(_missing, _peerWithMissingIds);
        _missingSequenceIds.push(_missing);
        continue;
      }
      // Up-to-date peer: no missing range, and stored patches go at least up to the last sequence id announced by the peer
      _peerStat[GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP] = _peerStat[LAST_PATCH_AT_TIMESTAMP];
      _peerStat[GUARANTEED_CONTIGUOUS_SEQUENCE_ID] = _peerStat[LAST_SEQUENCE_ID];
      _onSynced(_peerIdStr);
    }
    return _missingSequenceIds;
  }

  /**
   * Updates the stats of the peer and requests a missing sequence range to the peer which produced it.
   *
   * Missing ranges must be given sorted by peerId, sequenceId: the first missing range of a peer gives its contiguous sequence range.
   *
   * @param {Object} missing { peerId, sequenceId, patchedAt, nbMissingSequenceIds } sequenceId is the last received patch before the missing range
   * @param {Set<number>} peerWithMissingIds peers for which the contiguous sequence range is already updated
   */
  function _requestMissingPatches (missing, peerWithMissingIds) {
    const { peerId, sequenceId, nbMissingSequenceIds, patchedAt } = missing;
    if (peerStats[peerId] && !peerWithMissingIds.has(peerId)) {
      // Update peer stats. Missing patches are sorted by peerId, sequenceId.
      // So the first missing range gives us the contiguous sequence range.
      peerWithMissingIds.add(peerId);
      peerStats[peerId][GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP] = patchedAt; // TODO what happens if it's an uninitialized peerStat?
      peerStats[peerId][GUARANTEED_CONTIGUOUS_SEQUENCE_ID] = sequenceId;
    }
    if (peerSockets[peerId]) { // TODO the socket may not exist, need to ask another peer directly
      const _requestMessage = {
        type    : MESSAGE_TYPES.MISSING_PATCH,
        peer    : peerId,
        minSeq  : (sequenceId + 1),
        maxSeq  : (sequenceId + nbMissingSequenceIds),
        forPeer : myPeerId
      };
      nbRetransmissionRequestsSent = nbRetransmissionRequestsSent < Number.MAX_SAFE_INTEGER ? nbRetransmissionRequestsSent + 1 : 0;
      debugRetry('--> %d %o', peerId, _requestMessage);
      _sendMessageToPeer(peerId, JSON.stringify(_requestMessage));
    }
  }

  /**
   * Generates and broadcasts a ping stat message to all connected peers.
   *
   * This function is responsible for sending a ping message containing replication,
   * state, and sequence information to all peers in the cluster. The ping stat
   * message is used to synchronize sequence information and connectivity state.
   *
   * @param {boolean} [persist=false] - If true, generate a persistent ping stat message (saved in pending_patches table in all peers)
   * @returns {void}
   */
  function _generatePingStatMessage (persist = false) {
    if (persist === true) {
      return upsert(PENDING_PATCHES_TABLE_NAME, peerStats);
    }
    const _ping = {
      type  : MESSAGE_TYPES.PING,
      at    : lastPatchAtTimestamp,
      peer  : myPeerId,
      seq   : lastSequenceId,
      ver   : dbVersion,
      tab   : PENDING_PATCHES_TABLE_NAME,
      delta : peerStats
    };
    debugPing('--> all peers %o', _ping);
    _broadcast(_ping);
  }

  /**
   * Inserts or updates (upserts) a patch in the specified table and broadcasts it to all connected peers.
   *
   * This function creates a patch object from the supplied row data and table information,
   * increments the sequence ID, saves the patch, and applies it.
   *
   * It handles validation of required state (such as database version and initialization),
   * then notifies all connected peers of the new patch. If a callback is provided,
   * it is invoked upon completion with an error or with a generated session token.
   *
   * @param {string} tableName - The name of the table in which to upsert the patch.
   * @param {Object} rowPatch - The row data to use as the delta for the generated patch.
   * @param {function(Error|null, string=):void} [callback] - Optional callback invoked with error or the session token.
   */
  function upsert (tableName, rowPatch, callback) {
    if (!dbVersion) {
      return _onUpsertError(new Error('Database version is not set'), callback);
    }
    let _tableStatement = tableStatements[tableName];
    if (!_tableStatement) {
      return _onUpsertError(new Error(`Table ${tableName} not found`), callback);
    }
    if (lastSequenceId === -1) {
      return _onUpsertError(new Error('System not correctly initialized. Please call migrate first.'), callback);
    }
    const _patch = {
      type  : MESSAGE_TYPES.PATCH,
      at    : hlc.create(),
      peer  : myPeerId,
      seq   : lastSequenceId + 1,
      ver   : dbVersion,
      tab   : tableName,
      delta : _tableStatement.cleanRow(rowPatch)
    };
    try {
      _saveAndApplyPatches([_patch]);
    }
    catch (e) {
      console.error('Error processing patch %o ', _patch, e.message);
      return callback?.(e);
    }
    lastSequenceId++; // only increment lastSequenceId if patch is saved and applied
    lastPatchAtTimestamp = _patch.at;
    debugWrite('--> all peers %o', _patch);
    _broadcast(_patch);
    callback?.(null, _generateSessionToken(myPeerId, _patch.seq));
  }

  /**
   * Calls the optional upsert callback with the error, or logs the error if there is no callback
   *
   * @param {Error} err
   * @param {Function} [callback]
   * @private
   */
  function _onUpsertError (err, callback) {
    if (!callback) {
      console.error('Error in upsert', err.message);
      return;
    }
    return callback(err);
  }

  /**
   * Tests whether the database is correctly initialized and verifies that the rowPatch
   * can be correctly stored to and retrieved from the database.
   *
   * The extracted patch from the database should be the same as the inserted rowPatch.
   *
   * This function cannot be used in production because it creates a real patch.
   * However, it can be used (table by table) in tests to verify that everything is working as expected,
   * such as verifying that BigInts are converted to strings, BLOBs to strings, etc.
   *
   * Same usage as `upsert`.
   *
   * @param {*} tableName - The name of the table to test.
   * @param {*} rowPatch - The row data to insert and validate.
   * @param {function(Error|null)} callback - Called with an error if Replic-sqlite is not correctly initialized, otherwise null.
   */
  function selfTest (tableName, rowPatch, callback) {
    upsert(tableName, rowPatch, (err) => {
      if (err) {
        return callback(err);
      }
      const _missingPatch = globalStatements.getPatchFromColumn(myPeerId, 0, Number.MAX_SAFE_INTEGER);
      const _patchRetrieved = JSON.parse(_missingPatch[0].patch);
      const _knownColumns = tableStatements[tableName].knownColumns;
      const _knownColumnsSorted = [..._knownColumns].sort();
      try {
        assert.deepStrictEqual(Object.keys(_patchRetrieved.delta).sort(), _knownColumnsSorted);
        assert.deepStrictEqual(Object.keys(rowPatch).sort(), _knownColumnsSorted);
      }
      catch (e) {
        e.message += '\nReplic-sqlite: Please check that all columns are tested (no missing columns in the patch).';
        return callback(e);
      }
      try {
        assert.deepStrictEqual(_patchRetrieved.delta, rowPatch);
      }
      catch (e) {
        e.message += `\nReplic-sqlite: Please check that "prepareStatementHook" and "${tableName}_patches" table are correctly set to convert table columns for insert/extract operations (e.g., BigInt → string, BLOB → string), etc.\n`;
        return callback(e);
      }
      callback(null);
    });
  }

  /**
   * Handles patches received from connected peers.
   *
   * This function processes an incoming patch message from a remote peer.
   * It ignores self-generated patches, handles version mismatches by storing
   * incompatible patches in the pending_patches table, and applies valid patches
   * to the relevant table. Patches are buffered, then saved and applied all together in the next event loop.
   *
   * @param {Object} patch - The patch object received from a remote peer.
   *   @param {number|string} patch.peer - The peer ID that originated the patch.
   *   @param {number} patch.ver - The database version from the originating peer.
   *   @param {string} patch.tab - The table name to which the patch applies.
   *   @param {number} patch.at - The timestamp when the patch was created.
   */
  function _onPatchReceivedFromPeers (patch) {
    if (parseInt(patch.peer, 10) === myPeerId) {
      debug('Received patch from myself. Ignore it.');
      return;
    }
    if (patch.ver === dbVersion && !tableStatements[patch.tab]) {
      // Should never happen since patches with another dbVersion are stored in pending_patches
      console.warn(`Table ${patch.tab} not found when receiving patch from peers. Ignore patch.`);
      return;
    }
    // The next local patches must be more recent than the received ones, even if they are not applied yet
    hlc.receive(patch.at);
    if (receivedPatches.length === 0) {
      setImmediate(_flushReceivedPatches);
    }
    receivedPatches.push(patch);
  }

  /**
   * Saves and applies all buffered patches received from peers in one transaction.
   *
   * A patch is considered as received (peer stats) only once it is saved and applied.
   * Otherwise, it will be requested again later.
   */
  function _flushReceivedPatches () {
    const _patches = receivedPatches;
    receivedPatches = [];
    try {
      _saveAndApplyPatches(_patches);
    }
    catch {
      // Retry one by one to ignore only the invalid patches
      for (const _patch of _patches) {
        try {
          _saveAndApplyPatches([_patch]);
        }
        catch (e) {
          console.error('Error processing patch %o ', _patch, e.message);
          continue;
        }
        _detectMissingSequenceIds(_patch);
      }
      return;
    }
    for (const _patch of _patches) {
      _detectMissingSequenceIds(_patch);
    }
  }

  /**
   * Adds a remote peer to the replication system.
   *
   * This function registers a remote peer and its socket for communication.
   * Handles reconnection logic and initializes statistics for the new peer if needed.
   *
   * @param {number} remotePeerId - The ID of the remote peer to add.
   * @param {Object} socket - The socket object for the remote peer.
   * @param {Function} socket.send - Function to send messages to the peer.
   * @param {Function} socket.on - Function to register event listeners. Usage: socket.on('message', callback)
   * @param {Function} socket.off - Function to unregister event listeners. Usage: socket.off('message', callback)
   * @returns {void}
   */
  function addRemotePeer (remotePeerId, socket) {
    remotePeerId = parseInt(remotePeerId, 10); // we must enforce number to avoid bug with Set() peerStartedNotSynced
    if (isNaN(remotePeerId) || remotePeerId === myPeerId) {
      return;
    }
    // Initialize peer stats only if not already initialized
    if (!peerStats[remotePeerId]) {
      peerStats[remotePeerId] = [0, 0, 0, 0, 0, 0];
      peerStartedNotSynced.add(remotePeerId);
      debug('new remote peer %d', remotePeerId);
    }
    if (!peerSockets[remotePeerId]) {
      nbConnectedPeers++;
    }
    if (peerStats[remotePeerId]?._debounceCleanup) {
      debug('reconnect remote peer %d', remotePeerId);
      clearTimeout(peerStats[remotePeerId]._debounceCleanup);
      peerStats[remotePeerId]._debounceCleanup = null;
    }
    // If it is an existing connection, remove the old message listener
    peerSockets[remotePeerId]?.off?.('message', _onMessage);
    peerSockets[remotePeerId] = socket;
    socket?.on?.('message', _onMessage);
    _computeWhoIsTheLeader();
  }

  /**
   * Closes the connection to a remote peer.
   *
   * @param {number} remotePeerId
   * @returns {void}
   */
  function closeRemotePeer (remotePeerId) {
    remotePeerId = parseInt(remotePeerId, 10);
    if (isNaN(remotePeerId)) {
      return;
    }
    if (peerSockets[remotePeerId]) {
      nbConnectedPeers--;
      debug('close remote peer %d', remotePeerId);
      peerSockets[remotePeerId]?.off?.('message', _onMessage); // if it is an existing connection, we need to remove the old listener
      delete peerSockets[remotePeerId];
      Object.defineProperty(peerStats[remotePeerId], '_debounceCleanup', {
        value        : setTimeout(cleanDeadPeer, MAX_PEER_DISCONNECTION_TOLERANCE_MS, remotePeerId),
        writable     : true,
        configurable : true,
        enumerable   : false
      });
    }
  }

  /**
   * Cleans the stats of a remote peer. It is called after a timeout of MAX_PEER_DISCONNECTION_TOLERANCE_MS.
   *
   * @param {number} remotePeerId
   * @returns {void}
   */
  function cleanDeadPeer (remotePeerId) {
    delete peerStats[remotePeerId];
    _computeWhoIsTheLeader();
  }

  /**
   * Computes who is the leader among the connected peers.
   *
   * @returns {void}
   * @private
   */
  function _computeWhoIsTheLeader () {
    let _amITheLeader = true;
    for (const _peerIdStr in peerStats) {
      const _peerId = parseInt(_peerIdStr, 10);
      if (_peerId < myPeerId) {
        _amITheLeader = false;
        break;
      }
    }
    if (amITheLeaderCached !== _amITheLeader) {
      if (_amITheLeader === true) {
        debug('became leader');
        backupTask?.resume(); // manage backup cron
      }
      else {
        debug('lost leader role');
        backupTask?.pause();
      }
    }
    amITheLeaderCached = _amITheLeader;
  }

  /**
   * Checks if this peer is the leader (smallest peerId among connected peers)
   *
   * @returns {boolean} True if this peer is the leader
   */
  function amITheLeader () {
    if (amITheLeaderCached === null) {
      _computeWhoIsTheLeader();
    }
    return amITheLeaderCached;
  }

  /**
   * Detects and handles missing sequence IDs for a given incoming message from a peer (it must be fast)
   *
   * This function receives a message from a peer and checks whether there are any gaps
   * in the received sequence IDs for that peer. It updates internal statistics for the peer
   * and determines if there are contiguous or missing patches.
   *
   * TODO:
   *   - What to do if the missing packet is never received: the requesting peer should measure
   *     how long it remains stuck at the same sequence number. If the timeout is exceeded after querying
   *     all peers, it should skip and write in the pending packet the missing one, with a special tag.
   *   - At startup, peerStats should be left at zero to check if any patch is missing. All patches must be checked.
   *     Alternatively, just check the last hour; if a patch was missing, we would have already retrieved it.
   *
   * @param {Object} msg - The incoming network message containing sequence and peer information.
   */
  function _detectMissingSequenceIds (msg) {
    hlc.receive(msg.at);
    const _peerStat = peerStats[msg.peer];
    if (!_peerStat) {
      debug('receiving patch from unknown peer %d', msg.peer);
      return;
    }
    if (msg.type === MESSAGE_TYPES.PING) {
      _peerStat[MY_GUARANTEED_CONTIGUOUS_SEQUENCE_ID_STORED_IN_REMOTE_PEER] = msg?.delta?.[myPeerId]?.[GUARANTEED_CONTIGUOUS_SEQUENCE_ID] ?? 0;
    }
    // Keep track of the last message timestamp to detect peer liveness (non-persistent ping message)
    _peerStat[LAST_MESSAGE_TIMESTAMP] = Date.now();
    const _sequenceGap = msg.seq - _peerStat[GUARANTEED_CONTIGUOUS_SEQUENCE_ID];
    // A PING only announces the last sequence id of the peer, it does not carry the patch itself
    if (_sequenceGap === 1 && msg.type !== MESSAGE_TYPES.PING) {
      // Contiguous sequence, nothing to do
      _peerStat[GUARANTEED_CONTIGUOUS_SEQUENCE_ID] = msg.seq;
      _peerStat[GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP] = msg.at;
    }
    else if (_sequenceGap > 1) {
      // Detected a gap in the sequence. Do not update GUARANTEED_CONTIGUOUS_SEQUENCE_ID until contiguous again (this is managed by heartbeat processing)
      debug('missing patch for peer %d. Expects seq %d. Received seq %d', msg.peer, _peerStat[GUARANTEED_CONTIGUOUS_SEQUENCE_ID]+1, msg.seq);
    }
    if (msg.seq > _peerStat[LAST_SEQUENCE_ID]) {
      _peerStat[LAST_SEQUENCE_ID] = msg.seq;
      _peerStat[LAST_PATCH_AT_TIMESTAMP] = msg.at;
    }
    // Otherwise, it's an old packet, or a ping (same sequence), if so do nothing.
    // (Sequences can never go backward. If we receive a packet with a sequence less than the guaranteed one, we can ignore it—a double resend.)
  }

  /**
   * Deletes old patches from all patch tables based on the maximum patch retention policy.
   */
  function _deleteOldPatches () {
    const _oldestPatchTimestamp = hlc.from(Date.now() - MAX_PATCH_RETENTION_MS);
    for (const _table in tableStatements) {
      tableStatements[_table].deleteOldPatches(_oldestPatchTimestamp);
    }
  }

  /**
   * Detects missing patches from peers and requests their retransmission if needed.
   *
   * This function iterates over all peers to determine if there are any missing patches
   * based on the difference between the guaranteed contiguous sequence ID and the last known
   * sequence ID. The function finds the minimum guaranteed contiguous patch timestamp
   * among all peers that have missing patches. If such missing patches are detected,
   * it triggers a request to compute all ranges of missing patches in the database.
   *
   * For peers that are fully synchronized (their guaranteed contiguous sequence ID matches
   * their last sequence ID and the last message timestamp is valid), it invokes the `_onSynced` handler.
   */
  function _detectAndRequestMissingPatches () {
    let _highestMissingPatchTimestamp = Number.MAX_SAFE_INTEGER;
    for (const _peerId in peerStats) {
      const _peerStat = peerStats[_peerId];
      // find the minimum global guaranted contiguous timestamp among all peers
      if (_peerStat[GUARANTEED_CONTIGUOUS_SEQUENCE_ID] < _peerStat[LAST_SEQUENCE_ID] && _peerStat[GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP] < _highestMissingPatchTimestamp) {
        _highestMissingPatchTimestamp = _peerStat[GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP];
      }
      if (_peerStat[GUARANTEED_CONTIGUOUS_SEQUENCE_ID] === _peerStat[LAST_SEQUENCE_ID] && _peerStat[LAST_MESSAGE_TIMESTAMP] > 0) {
        _onSynced(_peerId);
      }
    }
    // if there are some missing patches, ask for them
    if (_highestMissingPatchTimestamp < Number.MAX_SAFE_INTEGER) {
      _getMissingPatches(_highestMissingPatchTimestamp);
    }
  }

  /**
   * Periodic maintenance function for cluster health and synchronization.
   *
   * This function is called at a regular interval as the cluster heartbeat.
   * It performs several maintenance tasks:
   *   - Deletes old patches from all patch tables approximately every hour, with random jitter (±5 minutes).
   *   - Generates and sends a persistent ping-stats message to all peers every hour
   *   - Sends a non-persistent ping-stats message to all peers at every heartbeat interval
   *   - Detects and requests missing patches from peers at every heartbeat interval.
   *   - Tracks and accumulates maintenance execution time in seconds.
   *
   * Timing logic uses random jitter to avoid coordination issues across peers.
   */
  function _heartBeat () {
    let _maintenanceTimeStart = performance.now();
    const _now = Date.now();
    const _jitter = Math.random() * HEARTBEAT_INTERVAL_MS;

    // An exception in setInterval would crash the process
    try {
      // Run _deleteOldPatches every hour with random jitter (±5 minutes)
      if ((_now - lastDeleteOldPatchesTimestamp) >= (3600 * 1000 + (Math.random() * 10 - 5) * 60 * 1000)) {
        lastDeleteOldPatchesTimestamp = _now;
        _deleteOldPatches();
        _generatePingStatMessage(true); // persistent ping stat (fake new patch)
      }
      else if ((_now - lastPingStatTimestamp) >= (HEARTBEAT_INTERVAL_MS + _jitter)) {
        lastPingStatTimestamp = _now;
        _generatePingStatMessage(false); // non-persistent ping stat
      }

      // Check for and request missing patches every heartbeat interval with jitter
      if ((_now - lastDetectMissingPatchesTimestamp) >= (HEARTBEAT_INTERVAL_MS + _jitter)) {
        lastDetectMissingPatchesTimestamp = _now;
        _detectAndRequestMissingPatches();
      }
    }
    catch (e) {
      console.error('Error in heartbeat', e.message);
    }

    const _elapsedMaintenanceTime = (performance.now() - _maintenanceTimeStart) / 1000;
    nbMaintenanceTimeSeconds = nbMaintenanceTimeSeconds < Number.MAX_SAFE_INTEGER ? nbMaintenanceTimeSeconds + _elapsedMaintenanceTime : 0;
  }


  /**
   * Handles a request from a remote peer for missing patches.
   * This function is called when another peer has detected a gap in its patch history
   * and is asking us to resend the missing patches for a given range.
   *
   * @param {Object} msg - The request message from the peer
   * @param {number} msg.peer - The peer ID requesting the patch
   * @param {number} msg.minSeq - The minimum sequence ID requested (inclusive)
   * @param {number} msg.maxSeq - The maximum sequence ID requested (inclusive)
   * @param {number} msg.forPeer - The peer ID to which the missing patches should be sent
   *
   * TODO: Return an error if the request is too old (patch table has been cleaned up)?
   */
  function _onRequestForMissingPatchFromPeers (msg) {
    let _missingPatch;
    try {
      _missingPatch = globalStatements.getPatchFromColumn(msg.peer, msg.minSeq, msg.maxSeq);
    }
    catch (e) {
      console.error('Error processing request for missing patch %o ', msg, e.message);
      return;
    }
    if (peerSockets[msg.forPeer]) {
      if (Array.isArray(_missingPatch)) {
        for (const _patch of _missingPatch) {
          debugRetry('--> %d %s', msg.forPeer, _patch.patch);
          _sendMessageToPeer(msg.forPeer, _patch.patch);
        }
        return;
      }
      if (_missingPatch) {
        debugRetry('--> %d %s', msg.forPeer, _missingPatch.patch);
        _sendMessageToPeer(msg.forPeer, _missingPatch.patch);
      }
    }
  }

  /**
   * Sends a message to a specific peer.
   * Handles incrementing the sent message counter, JSON parsing if needed,
   * and logs a debug message if the peer's socket cannot be found.
   *
   * @param {number} peerId - The peer ID to send the message to
   * @param {string} msgString - The message as a string (stringified JSON if not in string mode)
   */
  function _sendMessageToPeer (peerId, msgString) {
    nbMessagesSent = nbMessagesSent < Number.MAX_SAFE_INTEGER ? nbMessagesSent + 1 : 0;
    if (peerSockets[peerId]) {
      peerSockets[peerId].send(SOCKET_STRING_MODE ? msgString : JSON.parse(msgString));
    }
    else {
      debug('cannot send message to peer %d (peer socket not found)', peerId);
    }
  }

  /**
   * Handles incoming messages from connected peers.
   *
   * Parses and dispatches incoming network messages according to their type.
   * Handles PATCH (received patch from another peer), PING (peer connectivity and sequence checks), and MISSING_PATCH
   * (request from a remote peer for retransmission of a missing patch).
   *
   * If an unknown message type is received, and an `onUnknownMessage` callback is specified in options, it is called.
   *
   * The function also counts the number of received messages, and for retransmission requests,
   * the number of such requests received.
   *
   * TODO: Special case not developed yet:
   *   If a retransmission request is received for a patch not concerning us, and we are the peer with the lowest id,
   *   and the patch cannot be found, then we create a patch to stop the leak
   *   (stopping an infinite retransmission loop, for example when a peer wakes up after previously producing this patch).
   *
   * @param {Object|string} msg - The incoming message, either as an Object or as a JSON string (if SOCKET_STRING_MODE).
   */
  function _onMessage (msg, isBinary = false) {
    if (isBinary === true) {
      return options?.onUnknownMessage?.(msg, true);
    }
    if (SOCKET_STRING_MODE) {
      try {
        msg = JSON.parse(msg);
      }
      catch (e) {
        console.warn('Error parsing message', e);
        return;
      }
    }
    nbMessagesReceived = nbMessagesReceived < Number.MAX_SAFE_INTEGER ? nbMessagesReceived + 1 : 0;
    switch (msg.type) {
      case MESSAGE_TYPES.PATCH:
        debugWrite('<-- %o', msg);
        _onPatchReceivedFromPeers(msg);
        break;
      case MESSAGE_TYPES.PING:
        debugPing('<-- %o', msg);
        _detectMissingSequenceIds(msg);
        break;
      case MESSAGE_TYPES.MISSING_PATCH:
        debugRetry('<-- %o', msg);
        nbRetransmissionRequestsReceived = nbRetransmissionRequestsReceived < Number.MAX_SAFE_INTEGER ? nbRetransmissionRequestsReceived + 1 : 0;
        _onRequestForMissingPatchFromPeers(msg);
        break;
      default:
        options?.onUnknownMessage?.(msg, false);
        break;
    }
  }

  /**
   * Handles synchronization status update for a peer.
   *
   * This function is called when a peer is considered synced, i.e., when it has received
   * the last contiguous sequence for that peer. When a peer becomes synced
   * for the first time, it is removed from the `peerStartedNotSynced` set and
   * the 'synced' event is emitted for that peer.
   *
   * @param {number|string} peerId - ID of the peer that is now synced. May be a string if from a for-in loop.
   */
  function _onSynced (peerId) {
    // If all peers are already synced, exit early
    if (peerStartedNotSynced.size === 0) {
      return; // fast exit if everything is synced
    }
    debug('database synced with peer %d', peerId);
    const _peerStat = peerStats[peerId];
    const _peerId = parseInt(peerId, 10); // peerId can be a string when coming from "for in loop" and peerStartedNotSynced contains numbers
    // The node is synced if it has received at least one message from the peer
    if (peerStartedNotSynced.has(_peerId) && _peerStat?.[LAST_MESSAGE_TIMESTAMP] > 0) {
      peerStartedNotSynced.delete(_peerId);
      eventEmitter.emit('synced', _peerId);
    }
  }

  /**
   * Returns the current replication status of this peer.
   *
   * @returns {Object}
   */
  function status () {
    return {
      lastSequenceId,
      lastPatchAtTimestamp,
      peerStats
    };
  }

  /**
   * Parses a session token received from an untrusted (e.g., HTTP header) source and extracts the peer ID and sequence ID.
   *
   * The session token should be a string in the format "<peerId>.<sequenceId>", such as "100.42".
   * This is used for session-based consistency tracking, as in "read your write" middleware.
   *
   * Returns an object { peerId, sequenceId } if valid, otherwise returns null.
   *
   * @param {*} sessionToken - The session token provided by the client, typically from an HTTP header (may be a string or arbitrary value).
   * @returns {{peerId: number, sequenceId: number} | null} - The parsed peerId and sequenceId, or null if invalid.
   */
  function _parseSessionToken (sessionToken) {
    if (typeof sessionToken !== 'string' || sessionToken.length > 50) {
      return null;
    }
    const _sessionToken = sessionToken.split('.');
    const _peerId = parseInt(_sessionToken[0], 10);
    const _sequenceId = parseInt(_sessionToken[1], 10);
    if (_peerId > 0 && _peerId < Number.MAX_SAFE_INTEGER && _sequenceId > 0 && _sequenceId < Number.MAX_SAFE_INTEGER && _sessionToken.length === 2) {
      return {
        peerId     : _peerId,
        sequenceId : _sequenceId
      };
    }
    return  null;
  }

  /**
   * Generates a session token consisting of the peer ID and sequence ID.
   *
   * This function creates a session token string to be used for session-based consistency,
   * encoding the given peerId and sequenceId into the format "<peerId>.<sequenceId>".
   *
   * @param {number} peerId - The ID of the peer.
   * @param {number} sequenceId - The sequence ID associated with the peer.
   * @return {string} The generated session token, e.g., "100.42".
   */
  function _generateSessionToken (peerId, sequenceId) {
    return `${peerId}.${sequenceId}`;
  }

  /**
   * Checks whether the state for a given peer is consistent with the specified session token.
   *
   * This function determines if the database state has caught up to or surpassed the provided
   * sequence ID for a given peer. It is used to implement "read your write" consistency,
   * ensuring that a replica has received all patches from a particular session as indicated by
   * a session token.
   *
   * If the peer ID does not exist in the current peer stats (e.g., the peer is disconnected or does not exist),
   * the function considers the state as consistent for safety and usability.
   *
   * @param {number} peerId       The peer ID from which the session token originates.
   * @param {number} sequenceId   The minimum contiguous sequence ID that must be guaranteed to be applied (default: 0).
   * @returns {boolean}           True if the local state is at least as up-to-date as the given session token; false otherwise.
   */
  function isConsistentFromSessionToken (peerId, sequenceId = 0) {
    const _peerStat = peerStats[peerId];
    if (!peerId || !_peerStat) {
      return true; // It should never happen, except if reading a peer that does not exist anymore. In that case, we consider it is synced
    }
    if (_peerStat[GUARANTEED_CONTIGUOUS_SEQUENCE_ID] >= sequenceId) {
      return true;
    }
    return false;
  }

  /**
   * Express middleware to implement "read your write" consistency.
   *
   * This middleware checks whether the local database state is at least as up-to-date as the session token
   * provided in the request header. The session token encodes the peerId and sequenceId identifying the state
   * that must be observed for consistency. If the state is not yet consistent, the middleware waits (with backoff)
   * until the condition is met or a timeout occurs.
   *
   * @param {string} headerName The name of the HTTP header containing the session token (e.g., 'x-session-token').
   * @param {number} [timeout=5000] The maximum time (ms) to wait for consistency before timing out.
   * @returns {function} An Express-style middleware: (req, res, next) => void
   *
   * @details
   * If the session token is missing or invalid, the request is allowed to proceed immediately.
   * If the state is consistent (i.e., all relevant patches are applied), the request continues.
   * If the state is not yet consistent and the timeout is exceeded, the middleware calls next() with an error.
   */
  function readYourWrite (headerName, timeout = 5000) {
    return (req, res, next) => {
      const _sessionToken = req.headers[headerName];
      const _parsedSessionToken = _parseSessionToken(_sessionToken);
      if (!_parsedSessionToken) {
        return next(); // invalid or no session token, fast exit
      }
      backoff(
        () => {
          return isConsistentFromSessionToken(_parsedSessionToken.peerId, _parsedSessionToken.sequenceId);
        },
        success => {
          if (success) {
            return next();
          }
          nbReadYourWriteTimeouts++;
          return next(new Error('Replication consistency timeout'));
        },
        timeout
      );
    };
  }

  /**
   * Calls the provided function `fn` repeatedly (with exponential backoff) until it returns `true` or the specified timeout is reached.
   *
   * @param {Function} fn - Function to call repeatedly; should return `true` when the condition is met.
   * @param {Function} onDone - Callback invoked when the condition is met or on timeout: onDone(true) if success, onDone(false) if timeout.
   * @param {number} [maxTime=5000] - Maximum time in milliseconds to wait for `fn` to return true.
   * @param {number} [delay=10] - Initial delay between calls in milliseconds (doubles each retry, up to remaining time).
   * @param {number} [start=Date.now()] - Timestamp when backoff started (for internal use, do not set manually).
   */
  function backoff (fn, onDone, maxTime = 5000, delay = 10, start = Date.now()) {
    if (fn() === true) {
      return onDone(true); // success
    }
    if (Date.now() - start >= maxTime) {
      return onDone(false); // timeout
    }
    setTimeout(() => {
      backoff(fn, onDone, maxTime, Math.min(delay * 2, maxTime - (Date.now() - start)), start);
    }, delay);
  }

  /**
   * Generates a string containing OpenMetrics-compatible metrics for replication status and internal statistics.
   *
   * Each metric is labeled with the current peer's ID and, where appropriate, the remote peer's ID and direction.
   *
   * @returns {string} Multiline string in OpenMetrics/Prometheus exposition format.
   */
  function metrics () {
    const _clockDrift = hlc.getClockDriftMs();
    const _replicationLagMetrics = [];
    _replicationLagMetrics.push(`${METRICS_PREFIX}_replication_connected_peers{peer="${myPeerId}"} ${nbConnectedPeers}`);
    for (const _upstreamPeerId in peerStats) {
      const _peerStat = peerStats[_upstreamPeerId];
      const _lagMS = _peerStat[LAST_PATCH_AT_TIMESTAMP] > 0 ? hlc.toUnixTimestamp(_peerStat[LAST_PATCH_AT_TIMESTAMP]) - hlc.toUnixTimestamp(_peerStat[GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP]) : 0;
      _replicationLagMetrics.push(`${METRICS_PREFIX}_replication_lag_seconds{peer="${myPeerId}", remote_peer="${_upstreamPeerId}"} ${_lagMS / 1000.0}`);
    }
    _replicationLagMetrics.push(`${METRICS_PREFIX}_replication_retransmission_requests_total{peer="${myPeerId}", direction="sent"} ${nbRetransmissionRequestsSent}`);
    _replicationLagMetrics.push(`${METRICS_PREFIX}_replication_retransmission_requests_total{peer="${myPeerId}", direction="received"} ${nbRetransmissionRequestsReceived}`);
    _replicationLagMetrics.push(`${METRICS_PREFIX}_replication_messages_total{peer="${myPeerId}", direction="sent"} ${nbMessagesSent}`);
    _replicationLagMetrics.push(`${METRICS_PREFIX}_replication_messages_total{peer="${myPeerId}", direction="received"} ${nbMessagesReceived}`);
    _replicationLagMetrics.push(`${METRICS_PREFIX}_maintenance_time_seconds_total{peer="${myPeerId}"} ${nbMaintenanceTimeSeconds}`);
    _replicationLagMetrics.push(`${METRICS_PREFIX}_logical_clock_drift_max_seconds{peer="${myPeerId}"} ${_clockDrift / 1000.0}`);
    _replicationLagMetrics.push(`${METRICS_PREFIX}_read_your_write_timeouts_total{peer="${myPeerId}"} ${nbReadYourWriteTimeouts}`);
    _replicationLagMetrics.push(`${METRICS_PREFIX}_last_successful_backup_timestamp{peer="${myPeerId}"} ${lastSuccessfulBackupTimestamp}`);
    return _replicationLagMetrics.join('\n');
  }

  /**
   * Generates a unique peer id based on the current timestamp and a random number.
   *
   * The peer id is constructed as: (timestamp << 13) + randomInt
   * - The timestamp is the number of milliseconds since 2025-01-01.
   * - The randomInt is a number between 0 and 8090 (inclusive). 8091 is exclusive as the upper bound in crypto.randomInt.
   *
   * The peerId must be a valid 53-bit number below MAX_SAFE_INTEGER.
   *
   * @returns {number}
   */
  function generatePeerId () {
    const _timestampBits = Date.now() - hlc.UNIX_TIMESTAMP_OFFSET;
    const _timestampShifted = Number(BigInt(_timestampBits) << 13n);
    return _timestampShifted + crypto.randomInt(0, 8091);
  }

  /**
   * Checks if all connected peers have received all patches produced by this peer.
   *
   * If a peer is not connected, it is ignored (treated as having received all patches).
   * Only peers present in `peerSockets` are checked: `peerStats` keeps disconnected peers during MAX_PEER_DISCONNECTION_TOLERANCE_MS.
   * For each peer, if the highest sequence ID they have confirmed as stored
   * (MY_GUARANTEED_CONTIGUOUS_SEQUENCE_ID_STORED_IN_REMOTE_PEER) is less than this peer's
   * lastSequenceId, then not all patches are confirmed as replicated to that peer.
   *
   * @returns {boolean} True if all connected peers have received all patches produced by this peer.
   */
  function _areAllMyPatchesStoredByPeers () {
    for (const _peerId in peerSockets) {
      const _peerStat = peerStats[_peerId];
      if (_peerStat[MY_GUARANTEED_CONTIGUOUS_SEQUENCE_ID_STORED_IN_REMOTE_PEER] < lastSequenceId) {
        return false;
      }
    }
    return true;
  }

  /**
   * Initiates a controlled shutdown of the replic-sqlite instance.
   *
   * This function stops any scheduled backup task and initiates a graceful exit process.
   * If a callback is not provided, the function clears the heartbeat interval and exits immediately
   * without waiting for backup or data synchronization.
   *
   * Otherwise, it waits until all patches created by this peer have been fully replicated to the connected peers.
   * The heartbeat keeps running meanwhile: pings announce my last sequence id, so peers can request my lost patches.
   * If replication consistency is not reached within 5 times the heartbeat interval, the callback is called with an error.
   * Then, if the current node is the leader, it triggers a database backup of type 'shutdown' (even after a timeout).
   *
   * @param {Function} [callback] - Optional. Completion callback of the form (err, backupType, backupPath).
   *                                If not provided, shutdown proceeds immediately.
   */
  function exit (callback) {
    if (backupTask) {
      backupTask.stop();
    }
    if (!callback) {
      // leave immediately without waiting for backup / sync
      clearInterval(heartbeatInterval);
      return;
    }
    backoff(_areAllMyPatchesStoredByPeers, (isOk) => {
      clearInterval(heartbeatInterval);
      const _replicationError = isOk ? null : new Error('Replication consistency timeout');
      if (amITheLeader() === true) {
        return backupDatabase('shutdown', (err, backupType, backupPath) => callback(err ?? _replicationError, backupType, backupPath));
      }
      callback(_replicationError);
    }, 5 * HEARTBEAT_INTERVAL_MS);
  }

  return {
    myPeerId,
    exit,
    migrate,
    upsert,
    addRemotePeer,
    closeRemotePeer,
    status,
    event : eventEmitter,
    generatePeerId,
    metrics,
    backoff,
    readYourWrite,
    selfTest,
    amITheLeader,
    backupDatabase,
    _parseSessionToken,
    _generateSessionToken,
    _onPatchReceivedFromPeers,
    _flushReceivedPatches,
    _generateMergePatchesQueryPlan,
    _onRequestForMissingPatchFromPeers,
    _detectAndRequestMissingPatches,
    _getMissingPatches,
    _generatePingStatMessage,
    _deleteOldPatches,
    _heartBeat,
    _areAllMyPatchesStoredByPeers,
    _onSynced,

    backupTask,
    LAST_PATCH_AT_TIMESTAMP,
    LAST_SEQUENCE_ID,
    GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP,
    GUARANTEED_CONTIGUOUS_SEQUENCE_ID,
    LAST_MESSAGE_TIMESTAMP
  };
};

function findPreBuiltExtensionPath () {
  if (process.platform === 'win32') {
    throw new Error('Windows is not supported for database replication');
  }
  if (fs.existsSync(path.join(__dirname, '..', 'build', 'Release'))) {
    return path.join(__dirname, '..', 'build', 'Release', 'keep_last'); // load extension built by npm install
  }
  return path.join(__dirname, '..', 'dist', `keep_last-${process.platform}-${process.arch}`); // load extension packed in published module
}


module.exports = SQLiteOnSteroid;
