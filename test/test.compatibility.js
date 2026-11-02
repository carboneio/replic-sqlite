/**
 * Compatibility tests between the current version and the previous published version, extracted from git.
 * They are skipped if the git tag is not available (shallow clone).
 */
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const EventEmitter = require('events');
const { execFileSync } = require('child_process');
const Database = require('better-sqlite3');
const SQLiteOnSteroid = require('../lib/index.js');

const OLD_VERSION = 'v0.4.1';
const REPOSITORY_DIR = path.join(__dirname, '..');
// Inside node_modules, so that the old version finds its dependencies
const OLD_VERSION_DIR = path.join(REPOSITORY_DIR, 'node_modules', '.cache', `replic-sqlite-${OLD_VERSION}`);
const PRE_BUILT_EXTENSION_PATH = path.join(REPOSITORY_DIR, 'build', 'Release', 'keep_last');

const testSchema = `
  CREATE TABLE testA (
    id            INTEGER NOT NULL,
    tenantId      INTEGER NOT NULL,
    name          TEXT,
    PRIMARY KEY (id, tenantId)
  ) STRICT;

  CREATE TABLE testA_patches (
    _patchedAt    INTEGER  NOT NULL,
    _sequenceId   INTEGER  NOT NULL,
    _peerId       INTEGER  NOT NULL,
    id            INTEGER NOT NULL,
    tenantId      INTEGER NOT NULL,
    name          TEXT
  ) STRICT;

  CREATE INDEX testA_patches_at_idx ON testA_patches (_patchedAt);
`;

describe(`compatibility with ${OLD_VERSION}`, function () {
  const maxPeerDisconnectionToleranceMs = 10;
  let OldSQLiteOnSteroid, newDb, oldDb, newApp, oldApp, consoleError, errors;

  before(function () {
    OldSQLiteOnSteroid = loadOldVersion();
    if (!OldSQLiteOnSteroid) {
      console.log(`      git tag ${OLD_VERSION} not found: compatibility tests skipped`);
      this.skip();
    }
  });

  beforeEach(function () {
    errors = [];
    consoleError = console.error;
    console.error = (...args) => errors.push(args.join(' '));
    newDb = new Database(':memory:');
    oldDb = new Database(':memory:');
    newApp = SQLiteOnSteroid(newDb, 1, { maxPeerDisconnectionToleranceMs, preBuiltExtensionPath : PRE_BUILT_EXTENSION_PATH });
    oldApp = OldSQLiteOnSteroid(oldDb, 2, { maxPeerDisconnectionToleranceMs, preBuiltExtensionPath : PRE_BUILT_EXTENSION_PATH });
    newApp.migrate([{ up : testSchema, down : '' }]);
    oldApp.migrate([{ up : testSchema, down : '' }]);
  });

  afterEach(function () {
    console.error = consoleError;
    newApp?.exit();
    oldApp?.exit();
    newDb?.close();
    oldDb?.close();
  });

  it('should replicate in both directions, exchange pings with and without lost peers, and serve the patches of a dead peer', async function () {
    const _now = Date.now();
    // The dead peer 10 sent its patches to both peers, but the patch seq 2 is lost for the new peer
    newApp.addRemotePeer(10, { send : () => {} });
    oldApp.addRemotePeer(10, { send : () => {} });
    for (let seq = 1; seq <= 3; seq++) {
      const _patch = { type : 10, at : hlc(_now) + seq, peer : 10, seq, ver : 1, tab : 'testA', delta : { id : seq, tenantId : 10, name : `10-${seq}` } };
      oldApp._onPatchReceivedFromPeers(_patch);
      if (seq !== 2) {
        newApp._onPatchReceivedFromPeers(_patch);
      }
    }
    newApp.closeRemotePeer(10);
    oldApp.closeRemotePeer(10);
    await wait(maxPeerDisconnectionToleranceMs * 3);
    assert.ok(newApp.status().lostPeers[10], 'Peer 10 is a lost peer for the new version');

    connectPeers(newApp, oldApp);
    // Pings of the new version contain lost peers, pings of the old version do not
    newApp._generatePingStatMessage(false);
    newApp._generatePingStatMessage(true);
    oldApp._generatePingStatMessage(false);
    oldApp._generatePingStatMessage(true);
    await wait(20);
    // The new version requests the missing patch of the dead peer to the old version
    newApp._detectAndRequestMissingPatches();
    await wait(20);
    // Writes on both sides, including the same row
    newApp.upsert('testA', { id : 100, tenantId : 1, name : 'new' });
    oldApp.upsert('testA', { id : 200, tenantId : 2, name : 'old' });
    oldApp.upsert('testA', { id : 1, tenantId : 10, name : '10-1 updated by old' });
    await wait(20);

    const _expectedRows = [
      { id : 1, tenantId : 10, name : '10-1 updated by old' },
      { id : 2, tenantId : 10, name : '10-2' },
      { id : 3, tenantId : 10, name : '10-3' },
      { id : 100, tenantId : 1, name : 'new' },
      { id : 200, tenantId : 2, name : 'old' }
    ];
    assert.deepStrictEqual(readRows(newDb), _expectedRows);
    assert.deepStrictEqual(readRows(oldDb), _expectedRows);
    // Persistent pings are stored as patches of the pending_patches table, never applied
    const _oldPersistentPing = JSON.parse(oldDb.prepare('SELECT json(delta) AS delta FROM pending_patches WHERE _peerId = 1').get().delta);
    assert.ok(_oldPersistentPing[10], 'The old version stores the persistent ping with lost peers of the new version');
    assert.ok(newDb.prepare('SELECT count(*) AS nb FROM pending_patches WHERE _peerId = 2').get().nb >= 1, 'The old version also sends a persistent ping on connection');
    // Each version has received all the patches of the other one
    newApp._detectAndRequestMissingPatches(); // next heartbeat: the guaranteed contiguous sequence of the stored patches is updated
    assert.strictEqual(newApp.status().peerStats[2][newApp.GUARANTEED_CONTIGUOUS_SEQUENCE_ID], oldApp.status().lastSequenceId);
    assert.strictEqual(oldApp.status().peerStats[1][oldApp.GUARANTEED_CONTIGUOUS_SEQUENCE_ID], newApp.status().lastSequenceId);
    assert.strictEqual(newApp.status().lostPeers[10][newApp.GUARANTEED_CONTIGUOUS_SEQUENCE_ID], 3);
    assert.deepStrictEqual(errors, []);
  });
});

/**
 * Extracts the old version from git, and loads it
 *
 * @returns {Function|null} null if the git tag is not available
 */
function loadOldVersion () {
  try {
    for (const _file of ['lib/index.js', 'lib/hlc.js', 'lib/migrations/latest.sql']) {
      const _content = execFileSync('git', ['show', `${OLD_VERSION}:${_file}`], { cwd : REPOSITORY_DIR, stdio : ['ignore', 'pipe', 'ignore'] });
      const _destination = path.join(OLD_VERSION_DIR, _file);
      fs.mkdirSync(path.dirname(_destination), { recursive : true });
      fs.writeFileSync(_destination, _content);
    }
  }
  catch {
    return null;
  }
  return require(path.join(OLD_VERSION_DIR, 'lib', 'index.js'));
}

function connectPeers (appA, appB) {
  const _socketToB = new EventEmitter(); // used by appA to communicate with appB
  const _socketToA = new EventEmitter(); // used by appB to communicate with appA
  _socketToB.send = (msg) => setImmediate(() => _socketToA.emit('message', msg));
  _socketToA.send = (msg) => setImmediate(() => _socketToB.emit('message', msg));
  appA.addRemotePeer(appB.myPeerId, _socketToB);
  appB.addRemotePeer(appA.myPeerId, _socketToA);
}

function hlc (unixTimestamp) {
  return (unixTimestamp - 1735689600000) * (2 ** 13);
}

function readRows (db) {
  return db.prepare('SELECT * FROM testA ORDER BY id, tenantId').all();
}

function wait (ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
