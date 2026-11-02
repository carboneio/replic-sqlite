/**
 * Performance tests
 *
 * To compare with another version of the library:
 *   REPLIC_SQLITE_LIB=/absolute/path/to/lib/index.js ./node_modules/.bin/mocha test/test.perf.js --exit
 */
const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const EventEmitter = require('events');
const Database = require('better-sqlite3');
const SQLiteOnSteroid = require(process.env.REPLIC_SQLITE_LIB ?? '../lib/index.js');

const NB_PATCHES = 10000;
const NB_ROWS = 1000;
const NB_LOST_PEERS = 100;
const UNIX_TIMESTAMP_OFFSET = 1735689600000;
// Patches per second measured on Apple M4 Max, Node 24, to compare with the current machine
const REFERENCE_MACHINE = 'Apple M4 Max, Node 24';
const REFERENCE_PATCHES_PER_SECOND = {
  upsert                           : 36000,
  receive1PerLoop                  : 31000,
  receive100PerLoop                : 450000,
  receiveAllInOneLoop              : 750000,
  receiveAllInOneLoopWithLostPeers : 750000,
  receivePingWithLostPeers         : 300000
};

const testSchema = `
  CREATE TABLE testA (
    id            INTEGER NOT NULL,
    tenantId      INTEGER NOT NULL,
    name          TEXT,
    deletedAt     INTEGER,
    PRIMARY KEY (id, tenantId)
  ) STRICT;

  CREATE TABLE testA_patches (
    _patchedAt    INTEGER  NOT NULL,
    _sequenceId   INTEGER  NOT NULL,
    _peerId       INTEGER  NOT NULL,
    id            INTEGER NOT NULL,
    tenantId      INTEGER NOT NULL,
    name          TEXT,
    deletedAt     INTEGER
  ) STRICT;

  CREATE INDEX testA_patches_at_idx ON testA_patches (_patchedAt);
`;

describe('performance', function () {
  const dbPath = path.join(os.tmpdir(), `replic-sqlite-perf-${process.pid}.sqlite`);
  let db, app, socket2;

  beforeEach(function () {
    removeDatabase(dbPath);
    db = new Database(dbPath);
    db.pragma('journal_mode = WAL');
    db.pragma('synchronous = normal');
    app = SQLiteOnSteroid(db, 1, { preBuiltExtensionPath : path.join(__dirname, '..', 'build', 'Release', 'keep_last') });
    socket2 = new EventEmitter();
    socket2.send = () => {};
    app.addRemotePeer(2, socket2);
    app.migrate([{ up : testSchema, down : '' }]);
  });

  afterEach(function () {
    db.close();
    removeDatabase(dbPath);
  });

  it(`upsert ${NB_PATCHES} patches`, function () {
    const _start = process.hrtime.bigint();
    for (let i = 1; i <= NB_PATCHES; i++) {
      app.upsert('testA', { id : i % NB_ROWS, tenantId : 1, name : `name${i}` });
    }
    report(this.test.title, _start, REFERENCE_PATCHES_PER_SECOND.upsert);
    checkRows(db);
  });

  const _referencePerNbPatchesPerEventLoop = {
    1            : REFERENCE_PATCHES_PER_SECOND.receive1PerLoop,
    100          : REFERENCE_PATCHES_PER_SECOND.receive100PerLoop,
    [NB_PATCHES] : REFERENCE_PATCHES_PER_SECOND.receiveAllInOneLoop
  };
  for (const _nbPatchesPerEventLoop of [1, 100, NB_PATCHES]) {
    it(`receive ${NB_PATCHES} patches, ${_nbPatchesPerEventLoop} per event loop`, async function () {
      const _now = Date.now();
      const _start = process.hrtime.bigint();
      for (let i = 1; i <= NB_PATCHES; i++) {
        app._onPatchReceivedFromPeers(generatePatch(i, _now));
        if (i % _nbPatchesPerEventLoop === 0) {
          await new Promise(setImmediate);
        }
      }
      await new Promise(setImmediate);
      report(this.test.title, _start, _referencePerNbPatchesPerEventLoop[_nbPatchesPerEventLoop]);
      checkRows(db);
    });
  }

  it(`receive ${NB_PATCHES} patches, ${NB_PATCHES} per event loop, with ${NB_LOST_PEERS} lost peers`, async function () {
    const _now = Date.now();
    socket2.emit('message', generatePingWithLostPeers(_now));
    const _start = process.hrtime.bigint();
    for (let i = 1; i <= NB_PATCHES; i++) {
      app._onPatchReceivedFromPeers(generatePatch(i, _now));
    }
    await new Promise(setImmediate);
    report(this.test.title, _start, REFERENCE_PATCHES_PER_SECOND.receiveAllInOneLoopWithLostPeers);
    checkRows(db);
  });

  it(`receive ${NB_PATCHES} pings announcing ${NB_LOST_PEERS} lost peers`, function () {
    const _ping = generatePingWithLostPeers(Date.now());
    const _start = process.hrtime.bigint();
    for (let i = 1; i <= NB_PATCHES; i++) {
      socket2.emit('message', _ping);
    }
    report(this.test.title, _start, REFERENCE_PATCHES_PER_SECOND.receivePingWithLostPeers, 'pings');
  });
});

function generatePingWithLostPeers (now) {
  const _at = (now - UNIX_TIMESTAMP_OFFSET) * (2 ** 13);
  const _delta = { 1 : [_at, 0, _at, 0, 0, 0] };
  for (let i = 0; i < NB_LOST_PEERS; i++) {
    _delta[1000 + i] = [_at, 10, _at, 10, 0, 0];
  }
  return { type : 20, at : _at, peer : 2, seq : 0, ver : 1, delta : _delta };
}

function generatePatch (sequenceId, now) {
  return {
    type  : 10,
    at    : (now - UNIX_TIMESTAMP_OFFSET) * (2 ** 13) + sequenceId,
    peer  : 2,
    seq   : sequenceId,
    ver   : 1,
    tab   : 'testA',
    delta : { id : sequenceId % NB_ROWS, tenantId : 1, name : `name${sequenceId}` }
  };
}

function report (title, start, referencePerSecond, unit = 'patches') {
  const _ms = Number(process.hrtime.bigint() - start) / 1e6;
  console.log(`      ${title}: ${_ms.toFixed(0)} ms, ${Math.round(NB_PATCHES / _ms * 1000)} ${unit}/s (reference: ${referencePerSecond} ${unit}/s on ${REFERENCE_MACHINE})`);
}

function checkRows (db) {
  assert.strictEqual(db.prepare('SELECT count(*) AS nb FROM testA').get().nb, NB_ROWS);
  assert.strictEqual(db.prepare('SELECT count(*) AS nb FROM testA_patches').get().nb, NB_PATCHES);
  assert.strictEqual(db.prepare('SELECT name FROM testA WHERE id = 0').get().name, `name${NB_PATCHES}`);
}

function removeDatabase (dbPath) {
  for (const _suffix of ['', '-wal', '-shm']) {
    fs.rmSync(dbPath + _suffix, { force : true });
  }
}
