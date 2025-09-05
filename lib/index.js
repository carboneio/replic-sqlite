const fs = require('fs');
const path = require('path');
const debug = require('debug')('replic-sqlite');

const LAST_PATCH_AT_TIMESTAMP = 0;
const LAST_SEQUENCE_ID = 1;
const GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP = 2;
const GUARANTEED_CONTIGUOUS_SEQUENCE_ID = 3;

const STATUS_INITIALIZED = 100;
const PENDING_PATCHES_TABLE_NAME = '_'; // TODO
const MESSAGE_TYPES = {
  PATCH         : 10,
  PING          : 20,
  MISSING_PATCH : 30
};

/**
 * @param {import('better-sqlite3').Database} db
 * @param {number} myPeerId
 * @param {Object} options
 *                                            - socketStringMode : boolean
 *                                            - heartbeatIntervalMs : number
 *                                            - maxPatchRetentionMs : number
 *                                            - maxRequestForMissingPatches : number
 *                                            - prepareStatementHook : function
 *                                            - send(message)
 * 
 * @returns
 */
const SQLiteOnSteroid = (db, myPeerId = 1, options) => {
  if (!db) {
    throw new Error('Database instance is required');
  }

  db.loadExtension(path.resolve(__dirname, '../build/Release/keep_last'));
  
  // Options
  const SOCKET_STRING_MODE     = options?.socketStringMode ?? false;
  const HEARTBEAT_INTERVAL_MS  = options?.heartbeatIntervalMs ?? 0;  // Math.random() * 2000 + options?.heartbeatIntervalMs ?? (1000 * 5)); // 5-7 seconds with random jitter
  const PATCH_APPLY_DELAY_MS   = options?.patchApplyDelayMs ?? 10;
  const MAX_PATCH_RETENTION_MS = options?.maxPatchRetentionMs ?? 1000 * 60 * 60 * 25; // 25 hours
  const PREPARE_STATEMENT_HOOK = options?.prepareStatementHook ?? ((table, column) => '?');
  // Max number of missing patches to request from a peer, above this number, it request all patches from the minimum sequence
  const MAX_REQUEST_FOR_MISSING_PATCHES = options?.maxRequestForMissingPatches ?? 100; // TODO use it
  
  // Internal state for this peer
  let systemStatus = 0;
  let dbVersion = 1;
  let lastSequenceId = -1; // init and get from DB on restart
  let lastPatchAtTimestamp = -1;
  let totalPacketLossSinceMaxPatchAge = 0;
  let lastDeleteOldPatchesTimestamp = 0;
  let lastDetectMissingPatchesTimestamp = 0;
  let lastPingStatTimestamp = 0;

  const trackingMissingPatches = {};
  const peerSockets = {};
  const peerStats = {};
  const globalStatements = {
    // listMissingSequenceIds : list missing sequence ids of other peers (read all tables ending with _patches),
    // getLastPatchInfo       : get my latest sequence id and patchedAt timestamp stored in DB (read all tables ending with _patches),
  };
  const tableStatements = { 
    // <tableName> : {
    //   savePatch             : Function (patch)         => save patch in DB,
    //   applyPatches     : Function (fromTimestamp) => apply all patches from fromTimestamp to now on the table,
    //   debounceFromTimestamp : 0,
    //   debounceTimer         : null
    // }
    // short (efficient) reserved table name for internal use. '_' is a shortcut to the table 'pending_patches'
    '_' : {
      // savePatch = save pending patch in pending_patches table if patchVersion is not the same as the current dbVersion
    }
  };

  if (HEARTBEAT_INTERVAL_MS > 0) {
    setInterval(_heartBeat, HEARTBEAT_INTERVAL_MS);
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
    const _pkColumns = [];
    const _updateClauses = [];  
    for (const col of _tableInfo) {
      const _colName = col.name;
      let _valueExpr = PREPARE_STATEMENT_HOOK(tableName, _colName);
      _placeholders.push(_valueExpr);
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
        json_object('type', ${MESSAGE_TYPES.PATCH}, 'at', _patchedAt, 'peer', _peerId, 'seq', _sequenceId, 'ver', ${dbVersion}, 'tab', '${tableName}', 'delta', json_object(${_columns.map(col => `'${col}', ${col}`).join(', ')})) as patch
      FROM ${_tableNamePatches}
      WHERE _peerId = ?
        AND _sequenceId >= ?
        AND _sequenceId <= ?
    `;
    const _directUpsertSQLParamsFn = new Function('obj', `return [obj.${_columns.join(', obj.')}]`);
    const applyPatches     = db.prepare(_applyPatchesSQL);
    const savePatch        = db.prepare(_savePatchSQL);
    const deleteOldPatches = db.prepare(_generateDeleteOldPatchQuery(_tableNamePatches));
    const directUpsert     = db.prepare(_directUpsertSQL);
    tableStatements[tableName] = {
      applyPatches     : (fromTimestamp) => applyPatches.run(fromTimestamp),
      savePatch             : (patch) => savePatch.run(_savePatchSQLParamsFn(patch)),
      deleteOldPatches      : (timestamp) => deleteOldPatches.run(timestamp),
      // directUpsert          : (patch) => directUpsert.run(_directUpsertSQLParamsFn(patch)),
      debounceFromTimestamp : 0,
      debounceTimer         : null
    };
    return {
      applyPatchesSQL : _applyPatchesSQL,
      savePatchSQL         : _savePatchSQL,
      directUpsertSQL      : _directUpsertSQL,  
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
   * Prepare all statements needed to merge patches for all tables.
   * Called once just after migrations are applied.
   * 
   * Store all statements in the statements object.
   */
  function _prepareAllStatements () {
    const _listSequenceIds = [];
    const _getLastPatchInfo = [];
    const _getPatchFromColumn = [];
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
    const _patchTables = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%_patches' AND name != 'pending_patches'`).all();
    // Generate merge patches query plan for each table
    for (const table of _patchTables) {
      const _tableName = table.name.slice(0, -8); // Remove '_patches' suffix
      try {
        // Check if index exists on _patchedAt column
        const _indexExists = db.prepare(`SELECT 1 FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql LIKE '%_patchedAt%'`).get([table.name]);
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
      return (fromTimestamp = MAX_PATCH_RETENTION_MS) => {
        const _params = new Array(_nbTables).fill(fromTimestamp);
        return _plan.get(_params);
      };
    })();
    globalStatements.getPatchFromColumn = (() => {
      const _unionQuery = _getPatchFromColumn.join(' UNION ALL ');
      const _nbTables = _getPatchFromColumn.length;
      const _plan = db.prepare(`SELECT * FROM (${_unionQuery}) AS all_patches ORDER BY _sequenceId`);
      // TODO return error minSequenceID > à la séquence trouvé en BDD
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
      // Get all tables ending with _patches including pending_patches
      // TODO _listSequenceIds.push('SELECT _peerId AS peerId, _sequenceId AS sequenceId, _patchedAt AS patchedAt'));
      const _unionQuery = _listSequenceIds.join(' UNION ALL ');
      const _nbTables = _listSequenceIds.length;
      // faire des limit ddans l'union 
      // faire la technique du curseur, ramener 100 + 1 élément et avancer le curseur au fur et à mesure jusqu'à plus de ligne 
      const _plan = db.prepare(`SELECT * FROM (
                                  SELECT *, (lead(sequenceId, 1) OVER (PARTITION BY peerId ORDER BY sequenceId)) - sequenceId - 1 AS nbMissingSequenceIds FROM (${_unionQuery})
                                ) WHERE nbMissingSequenceIds > 0 ORDER BY peerId, sequenceId`);
      return (fromTimestamp) => {
        const _params = new Array(_nbTables).fill(fromTimestamp);
        const _result = _plan.all(_params);
        return _result;
      };
    })();
  }

  function _initPeerSequence () {
    const _stat = globalStatements.getLastPatchInfo(MAX_PATCH_RETENTION_MS); // TODO limit search to last 1000 patches
    if (_stat?.sequenceId > 0) {
      lastSequenceId = _stat.sequenceId;
      lastPatchAtTimestamp = _stat.patchedAt;
      return;
    }
    lastSequenceId = 0;
    lastPatchAtTimestamp = 0;
  }

  /**
   * Migrate the database to the latest version.
   * 
   * TODO udpate existing down migration is DB
   * 
   * @param {Array}    appMigrations [{ up : string, down : string, upPatch : Function, downPatch : Function }]
   * @returns {Object} { currentVersion : number, previousVersion : number }
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
    systemStatus = STATUS_INITIALIZED;

    return {
      currentVersion: _targetId,
      previousVersion: _lastAppliedId
    };
  }

  // TODO test and implement after migration
  function _applyPendingPatches () {
    const _filter = ` tableName <> ${PENDING_PATCHES_TABLE_NAME} AND patchVersion = ${dbVersion} `;
    const _pendingPatches = db.prepare(_generateGetPatchFromPendingTableQuery(_filter)).all();
    // Transfer pending patches to the main tables in one atomic transaction
    const _transferPendingPatches = db.transaction(() => {
      for (const _patch of _pendingPatches) {
        const _patchObj = JSON.parse(_patch.patch);
        let _tableStatement = statements[_patchObj.tab];
        if (_tableStatement) {
          _tableStatement.savePatch(_patchObj);
        }
        else {
          console.warn(`Table ${_patchObj.tab} not found when applying pending patches. Ignore patch.`);
        }
      }
      db.prepare(`DELETE FROM pending_patches WHERE ${_filter}`).run();
    });
    _transferPendingPatches();
    // TODO TODO apply patch table per table
  }
  
  function _broadcast (msg) {
    for (const _peerId in peerSockets) {
      const _peer = peerSockets[_peerId];
      _peer.send(SOCKET_STRING_MODE ? JSON.stringify(msg) : msg);
    }
  }


  // TODO if greater than 100 ask for the whole range
  function _getMissingPatches (fromTimestamp) {
    const _missingSequenceIds = globalStatements.listMissingSequenceIds(fromTimestamp);
    const _peerWithMissingIds = new Set();
    // Send messages to request missing patches from peers
    for (let i = 0; i < _missingSequenceIds.length; i++) {
      const _missing = _missingSequenceIds[i];
      const { peerId, sequenceId, nbMissingSequenceIds, patchedAt } = _missing;
      if (peerSockets[peerId]) { // TODO la socket peut ne pas exister, il faut demander directement à un autre peer
        if (!_peerWithMissingIds.has(peerId)) {
          // Update peer stats. Missing patch are sorted by peerId, sequenceId
          _peerWithMissingIds.add(peerId);
          peerStats[peerId][GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP] = patchedAt; // TODO qu'est ce qui se passe si c'est un peerStat non initialisé ?
          peerStats[peerId][GUARANTEED_CONTIGUOUS_SEQUENCE_ID] = sequenceId;
        }
        const _requestMessage = {
          type    : MESSAGE_TYPES.MISSING_PATCH,
          peer    : peerId,
          minSeq  : (sequenceId + 1),
          maxSeq  : (sequenceId + nbMissingSequenceIds),
          forPeer : myPeerId
        };
        peerSockets[peerId].send(SOCKET_STRING_MODE ? JSON.stringify(_requestMessage) : _requestMessage);
      }
    }
    // Update peer stats for peers that have no missing patches (up to date peers)
    for (const _peerId in peerStats) {
      if (!_peerWithMissingIds.has(parseInt(_peerId, 10))) {
        peerStats[_peerId][GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP] = peerStats[_peerId][LAST_PATCH_AT_TIMESTAMP];
        peerStats[_peerId][GUARANTEED_CONTIGUOUS_SEQUENCE_ID] = peerStats[_peerId][LAST_SEQUENCE_ID];
      }
    }
    return _missingSequenceIds;
  }

  /*function getNextPeerToAskForMissingPatches (missingSequenceRange, peerSockets) {
    const _now = Date.now();
    const _missingFromPeer = trackingMissingPatches[missingSequenceRange.peer];
    if (!_missingFromPeer) {
      trackingMissingPatches[missingSequenceRange.peer] = {
        lastMessageSentAt : Date.now(),
        lowestSeq         : missingSequenceRange.minSeq,
        // highestSeq        : missingSequenceRange.maxSeq,
        askedTo           : new Set()
      };
    }
    else if (missingFromPeer.lowestSeq > missingSequenceRange.minSeq) {
      _missingFromPeer.lowestSeq = missingSequenceRange.minSeq;
      _missingFromPeer.maxSeq    = missingSequenceRange.maxSeq;
      _missingFromPeer.lastMessageSentAt = Date.now();
    }
    else if (_missingFromPeer.lastMessageSentAt > _now - MAX_REQUEST_FOR_MISSING_PATCHES_INTERVAL_MS) {
      _missingFromPeer.lastMessageSentAt
    }
    else {
      // insérer dans pending_pacth avec timestamp à 0 
    }
    if (peerSockets[requestMessage.peer]) {

    }
    trackingMissingPatches[requestMessage.peer] = {requestMessage.at};
    peerSockets[requestMessage.peer].lastMissingPatchTimestamp = requestMessage.at;
    peerSockets[peerId].lastMissingPatchTimestamp = requestMessage.at;
    for (const _peerId in peerStats) {
      if (peerStats[_peerId][GUARANTEED_CONTIGUOUS_SEQUENCE_ID] < peerStats[_peerId][LAST_SEQUENCE_ID] && peerStats[_peerId][GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP] < _highestMissingPatchTimestamp) {
        return _peerId;
      }
    }
  }*/

  function _generatePingStatMessage (persist = false) {
    if (persist === true) {
      return upsert(PENDING_PATCHES_TABLE_NAME, peerStats);
    }
    _broadcast({
      type  : MESSAGE_TYPES.PING,
      at    : lastPatchAtTimestamp,
      peer  : myPeerId,
      seq   : lastSequenceId,
      ver   : dbVersion,
      tab   : PENDING_PATCHES_TABLE_NAME,
      delta : peerStats
    });
  }

  function upsert (tableName, rowPatch, callback) {
    if (!dbVersion) {
      return callback(new Error('Database version is not set'));
    }
    let _tableStatement = tableStatements[tableName];
    if (!_tableStatement) {
      return callback(new Error(`Table ${tableName} not found`));
    }
    if (lastSequenceId === -1) {
      return callback(new Error(`System not correctly initialized. Please call migrate first.`));
    }
    const _patch = {
      type  : MESSAGE_TYPES.PATCH,
      at    : Date.now(),
      peer  : myPeerId,
      seq   : lastSequenceId + 1,
      ver   : dbVersion,
      tab   : tableName,
      delta : rowPatch
    };
    _tableStatement.savePatch(_patch); // TODO insert by batch top optimize a bit
    lastSequenceId++; // only increment lastSequenceId if patch is saved. TODO add tests. Make it cyclic, increment time when it switch
    lastPatchAtTimestamp = _patch.at;
    _tableStatement?.applyPatches?.(_patch.at); // It is not executed if patch is stored only in pending_patches table (happens when a ping stat message is generated internally)
    _broadcast(_patch);
    callback?.();
  }

  function _onPatchReceivedFromPeers (patch) {
    if (parseInt(patch.peer, 10) === myPeerId) {
      debug('Received patch from myself. Ignore it.');
      return;
    }
    if (patch.ver !== dbVersion) {
      // if version mismatch, save it in pending_patches table for later processing
      tableStatements[PENDING_PATCHES_TABLE_NAME].savePatch(patch);
      _detectMissingSequenceIds(patch);
      return;
    }
    let _tableStatement = tableStatements[patch.tab];
    if (!_tableStatement) {
      // should never happen since we only send patches to peers with the same dbVersion (manage above)
      console.warn(`Table ${patch.tab} not found when receiving patch from peers. Ignore patch.`);
      return;
    }
    _tableStatement.savePatch(patch);
    _detectMissingSequenceIds(patch);

    // update real table, if it is not a ping message generated internally (stored only in pending_patches) with the short and reserved table name '_'
    if (_tableStatement.applyPatches) {
      // debounce
      if (patch.at < _tableStatement.debounceFromTimestamp) {
        _tableStatement.debounceFromTimestamp = patch.at; // store the timestamp of the oldest patch to apply for the debouncer
      }
      // TODO apply at least once per second (if burst <- neveer applyied)
      clearTimeout(_tableStatement.debounceTimer);
      _tableStatement.debounceTimer = setTimeout(_tableStatement.applyPatches, PATCH_APPLY_DELAY_MS, _tableStatement.debounceFromTimestamp);
    }
  }


  function addRemotePeer(remotePeerId, socket, connectionInfo) {
    if (remotePeerId === myPeerId) {
      return;
    }
    // init peer stats only if not already initialized
    if (!peerStats[remotePeerId]) {
      peerStats[remotePeerId] = [0,0,0,0]; 
      // enregistrer régulièrement des checkpoint dans pending°-Patches pour éviter de rtourner trop tpot à chaque démarrage. Se servir de ça pour éviter les boucle inf ?
      debug('addRemotePeer', remotePeerId);
    }
    // TODO test
    peerSockets[remotePeerId]?.off?.('message', _onMessage); // if it is a new connection, we need to remove the old listener
    peerSockets[remotePeerId] = socket;
    socket?.on?.('message', _onMessage);
  }

  function _detectMissingSequenceIds (msg) {
    // TODO QUe faire si on ne recoit jamais le mpaquet manquant => celui qui demande doit mesurer le temps où on recette bloqué toujours à la même séquence
    //   Si ça commence à être trop long après avoir demander à tous les peer, on saute on écrit dans pending paquet, le paquet manquant avec un tag particulier
    // Check au startup, peerStats doit êter laisser à zero pour vérifier qu'il ne manque aucun patch, on est obligé de tout parcourir
    //  -> Ou juste checker la dernière heure. Si un patch manquait, on laurait déjà récupérer.
    const _peerStat = peerStats[msg.peer];
    if (!_peerStat) {
      debug('reveiving patch from unknown peer %d', msg.peer);
      return;
    }
    const _sequenceGap = msg.seq - _peerStat[GUARANTEED_CONTIGUOUS_SEQUENCE_ID];
    if (_sequenceGap === 1) {
      // contiguous sequence, nothing to do
      _peerStat[GUARANTEED_CONTIGUOUS_SEQUENCE_ID] = msg.seq;
      _peerStat[GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP] = msg.at;
    }
    else if (_sequenceGap > 1) {
      // jump into the sequence. Do not update GUARANTEED_CONTIGUOUS_SEQUENCE_ID until we are contiguous again (updated by heartbeat processing)
      debug('missing patch for peer %d. Expects seq %d. Received seq %d', msg.peer, _peerStat[GUARANTEED_CONTIGUOUS_SEQUENCE_ID]+1, msg.seq);
    }
    if (msg.seq > _peerStat[LAST_SEQUENCE_ID]) {
      _peerStat[LAST_SEQUENCE_ID] = msg.seq;
      _peerStat[LAST_PATCH_AT_TIMESTAMP] = msg.at;
    }
    // SINON c'est un vieux paquet, ou un ping (same sequence), on ne fait rien (la sequence ne peut jamais reculer) (gérer faille sécu si on reçoit un paquet avec un eséquence plus grande que al raélité)
    // Et d'ailleurs, si on recçoit un paque dons la sequence est avant la séquence guarantee, on peut l'ignore (double resend)
  }
  
  function _deleteOldPatches () {
    const _oldestPatchTimestamp = Date.now() - MAX_PATCH_RETENTION_MS;
    for (const _table in tableStatements) {
      tableStatements[_table].deleteOldPatches(_oldestPatchTimestamp);
    }
  }

  function _detectAndRequestMissingPatches () {
    let _highestMissingPatchTimestamp = Number.MAX_SAFE_INTEGER;
    for (const _peerId in peerStats) {
      const _peerStat = peerStats[_peerId];
      // find the minimum global guaranted contiguous timestamp among all peers
      if (_peerStat[GUARANTEED_CONTIGUOUS_SEQUENCE_ID] < _peerStat[LAST_SEQUENCE_ID] && _peerStat[GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP] < _highestMissingPatchTimestamp) {
        _highestMissingPatchTimestamp = _peerStat[GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP];
      }
    }
    // if there are some missing patches, ask for them
    if (_highestMissingPatchTimestamp < Number.MAX_SAFE_INTEGER) {
      _getMissingPatches(_highestMissingPatchTimestamp);
    }
  }

  // effacer les vieux patch toutes les heures
  // insérer un ping stat mesage toutes les heures
  // faire un backup tournant à une heure précise
  // dans le sstatus, retourner aussi la version de DB de chaque peer, la quantité de patch manquant, la 
  // quiter le plus rapidement possinle
  // ne pas lancer un atrtement 2 fois, protéger le heartbeat entierement (sychrone donc pas de risque ?)
  function _heartBeat () {
    const _now = Date.now();
    const _jitter = Math.random() * HEARTBEAT_INTERVAL_MS;

    // Run _deleteOldPatches every hour with random jitter (+- 5 minutes)
    if ((_now - lastDeleteOldPatchesTimestamp) >= (3600 * 1000 + (Math.random() * 10 - 5) * 60 * 1000)) {
      _deleteOldPatches();
      _generatePingStatMessage(true); // persistant stat (fake new patch)
      lastDeleteOldPatchesTimestamp = _now;
    }
    else if ((_now - lastPingStatTimestamp) >= (HEARTBEAT_INTERVAL_MS + _jitter)) {
      _generatePingStatMessage(false);
      lastPingStatTimestamp = _now;
    }

    if ((_now - lastDetectMissingPatchesTimestamp) >= (HEARTBEAT_INTERVAL_MS + _jitter)) {
      _detectAndRequestMissingPatches();
      lastDetectMissingPatchesTimestamp = _now;
    }
    
    // TODO
    // Run _backupDatabase daily at specific time with random jitter
    // const _backupHour = 2; // 2 AM
    // const _currentHour = new Date().getHours();
    // if (_currentHour === _backupHour && (_now - lastBackup) >= (24 * 60 * 60 * 1000)) {
    //   lastBackup = _now;
    // }
  }
  

  function _onRequestForMissingPatchFromPeers (msg) {
    // TODO retourner une erreur si la demande est trop vieilel (patch table nettoyée)
    // TODO limiter   msg.maxSeq  (max paquet de 100, 1000 ?)
    debug('onRequestForMissingPatchFromPeers', msg);
    const _missingPatch = globalStatements.getPatchFromColumn(msg.peer, msg.minSeq, msg.maxSeq);
    if (peerSockets[msg.forPeer]) {
      if (Array.isArray(_missingPatch)) {
        for (const _patch of _missingPatch) {
          peerSockets[msg.forPeer].send(SOCKET_STRING_MODE ? _patch.patch : JSON.parse(_patch.patch));
        }
        return;
      }
      if (_missingPatch) {
        peerSockets[msg.forPeer].send(SOCKET_STRING_MODE ? _missingPatch.patch : JSON.parse(_missingPatch.patch));
      }
    }
  }

  function _onMessage(msg) {
    
    if (SOCKET_STRING_MODE) {
      debug('onMessage %s', msg);
      try {
        msg = JSON.parse(msg);
      }
      catch (e) {
        console.warn('Error parsing message', e);
        return;
      }
    }
    else {
      debug('onMessage %o', msg);
    }
    switch (msg.type) {
      case MESSAGE_TYPES.PATCH:
        _onPatchReceivedFromPeers(msg);
        break;
      case MESSAGE_TYPES.PING:
        _detectMissingSequenceIds(msg);
        break;
      case MESSAGE_TYPES.MISSING_PATCH:
        // si on reçoit une demande qui ne nous concerne pas et que nous somme l'idPeer le plus petit, et qu'on ne trouve pas le patch, alors on en créé un 
        // pour stopper l'hémoragie (stopper la boucle infinie, au réveil du peer qui avait produit se patch)...
        _onRequestForMissingPatchFromPeers(msg);
        break;
      default:
        console.warn('Unknown message type', msg.type);
        break;
    }
  }

  function closeRemotePeer(remotePeerId) {
    delete peerSockets[remotePeerId];
  }

  function status () {
    return {
      lastSequenceId,
      lastPatchAtTimestamp,
      peerStats
    };
  }
  

  return {
    // promise : // promise interface
    migrate,
    upsert,
    addRemotePeer,
    closeRemotePeer,
    status,
  
    _onPatchReceivedFromPeers,
    _generateMergePatchesQueryPlan,
    _onRequestForMissingPatchFromPeers,
    _detectAndRequestMissingPatches,
    _getMissingPatches,
    _generatePingStatMessage,
    _deleteOldPatches,
    _heartBeat,

    LAST_PATCH_AT_TIMESTAMP,
    LAST_SEQUENCE_ID,
    GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP,
    GUARANTEED_CONTIGUOUS_SEQUENCE_ID
  }
};


module.exports = SQLiteOnSteroid;
