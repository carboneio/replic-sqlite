const assert = require('assert');
const SQLiteOnSteroid = require('../lib/index.js');
const path = require('path');
const fs = require('fs');
const Database  = require('better-sqlite3');
const debug = require('debug');
const EventEmitter = require('events');
const { simplifyStats } = require('./helper.js');
const hlc = require('../lib/hlc.js');

const PING = 20;
const PATCH = 10;
const MISSING_PATCH = 30;

describe('main', function () {
  const _testSchema = `
    CREATE TABLE testA (
      id            INTEGER NOT NULL,
      tenantId      INTEGER NOT NULL,
      name          TEXT,
      PRIMARY KEY (id, tenantId)
    ) STRICT;
    
    CREATE TABLE testA_patches (
      _patchedAt    INTEGER  NOT NULL, /* 53bits number (Unix timestamp in millisecond) -*/
      _sequenceId   INTEGER  NOT NULL, /* Consecutive sequence number of change per peer */
      _peerId       INTEGER  NOT NULL, /* 32bits globally unique, Source of change */
      
      id            INTEGER NOT NULL,
      tenantId      INTEGER NOT NULL,
      name          TEXT
    ) STRICT;

    CREATE INDEX testA_patches_at_idx ON testA_patches (_patchedAt);
  `;

  afterEach (function () {
    hlc._reset();
  });

  describe('startup + should generate a peerId automatically if not provided', function () {
    it ('should generate a peer id if not provided', function () {
      const db = connect(); // memory db
      const app = SQLiteOnSteroid(db, null, {  });
      assert.strictEqual(app.myPeerId > 1000000, true);
    });
    it ('should use the provided peer id', function () {
      const db = connect(); // memory db
      const app = SQLiteOnSteroid(db, 1, {  });
      assert.strictEqual(app.myPeerId, 1);
    });
  });

  describe('startup + receives ping indicating that the node is not up to date.', function () {
    let db, app;
    const _eventEmitter100 = new EventEmitter();
    const _eventEmitter200 = new EventEmitter();
    let messageSentToPeer100 = [];
    let messageSentToPeer200 = [];
    beforeEach (function () {
      messageSentToPeer100 = [];
      messageSentToPeer200 = [];
      db = connect(); // memory db
      app = SQLiteOnSteroid(db, 1, {  });
      app.migrate([{ up : _testSchema, down : ''}]);
      _eventEmitter100.send = (message) => messageSentToPeer100.push(JSON.parse(JSON.stringify(message)));
      _eventEmitter200.send = (message) => messageSentToPeer200.push(JSON.parse(JSON.stringify(message)));
      const fakePeerSockets = {
        100 : _eventEmitter100,
        200 : _eventEmitter200
      };
      app.addRemotePeer(100, fakePeerSockets[100]);
      app.addRemotePeer(200, fakePeerSockets[200]);
    });
    afterEach (function (done) {
      setTimeout(() => {
        close(db); // let the job that write the patched table finished
        done();
      }, 20);
    });

    it.skip('should store stats in pending_patches table', function (done) {
      const _now = Date.now();
      const _status = app.status();
      assert.deepStrictEqual(_status.lastSequenceId, 0);
      _eventEmitter100.emit('message', { type : PING, at : _now-1, peer : 100, seq : 54, ver : 1, tab : '_', delta : {} });
      _eventEmitter100.emit('message', { type : PING, at : _now  , peer : 100, seq : 55, ver : 1, tab : '_', delta : {} });
      assert.deepStrictEqual(_status.peerStats[100], [_now, 55, 0, 0]);
      assert.deepStrictEqual(_status.lastSequenceId, 0);
      heartBeatSync(app);
      assert.deepStrictEqual(_status.peerStats[100], [_now, 55, 0, 0]); // PAS BON !!! (faut générer un patch vide dans le get missing patch)
      setTimeout(() => {
        // const _pendingPatches = db.prepare('SELECT * FROM pending_patches').all();
        // assert.strictEqual(_pendingPatches.length, 1, 'There should be one pending patch');
        // const _patch = _pendingPatches[0];
        // assert.strictEqual(_patch._peerId, 100, 'Pending patch should be for peer 100');
        // assert.strictEqual(_patch._sequenceId, 55, 'Pending patch should have sequenceId 55');
        // assert.strictEqual(_patch.tableName, '_', 'Pending patch should be for table "_"');
        // assert.strictEqual(_patch.patchVersion, 1, 'Patch version should be 1');
        // // TODO
        done();
      }, 100);
    });

    it('should insert rows as soon as possible and request missing patch', function (done) {
      const _syncedPeers = [];
      app.event.on('synced', (peerId) => {
        _syncedPeers.push(peerId);
      });
      const _now = Date.now();
      const _status = app.status();
      assert.deepStrictEqual(_status.lastSequenceId, 0);
      _eventEmitter100.emit('message', { type : PATCH, at : hlc.from(_now-2), peer : 100, seq : 1, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 1, name : '1a' } });
      // node 200 send a message at the same time as node 100, node 200 wins (higher node id wins)
      _eventEmitter200.emit('message', { type : PATCH, at : hlc.from(_now-1), peer : 200, seq : 1, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 1, name : '1c' } });
      _eventEmitter100.emit('message', { type : PATCH, at : hlc.from(_now-1), peer : 100, seq : 2, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 1, name : '1b' } });
      _eventEmitter100.emit('message', { type : PATCH, at : hlc.from(_now), peer : 100, seq : 5, ver : 1, tab : 'testA', delta : { id : 5, tenantId : 1, name : '5a' } });
      assert.deepStrictEqual(_status.peerStats[100].slice(0,-2), [hlc.from(_now), 5, hlc.from(_now-1), 2]);
      // should insert rows as soon as possible
      setTimeout(() => {
        assert.deepStrictEqual(messageSentToPeer100.length, 0);
        const _tableARows = db.prepare('SELECT * FROM testA').all();
        assert.strictEqual(_tableARows.length, 2);
        assert.deepStrictEqual(_tableARows[0], { id : 1, tenantId : 1, name : '1c' });
        assert.deepStrictEqual(_tableARows[1], { id : 5, tenantId : 1, name : '5a' });
        assert.deepStrictEqual(_syncedPeers, []);
        heartBeatSync(app);
        assert.deepStrictEqual(_syncedPeers, [200]);
        // Check that a message was sent to peer 100 for the missing patch (seq 3)
        assert.strictEqual(messageSentToPeer100.length, 2, 'Should have sent two messages to peer 100 for missing patch');
        const _persistentPingLastTimestamp =  messageSentToPeer100[0].delta['100'][4];
        const _persistentPingPatchAt = messageSentToPeer100[0].at;
        assert.deepStrictEqual(simplifyStats(messageSentToPeer100), [
          { type : PATCH, at : _persistentPingPatchAt, peer : 1, seq : 1, ver : 1, tab : '_', delta : { '100' : [hlc.from(_now), 5, hlc.from(_now - 1), 2, _persistentPingLastTimestamp, 0], '200' : [hlc.from(_now-1), 1, hlc.from(_now-1), 1, _persistentPingLastTimestamp, 0] } },
          { type : MISSING_PATCH, peer : 100, minSeq : 3, maxSeq : 4, forPeer : 1 }
        ]);

        // accept duplicated message
        _eventEmitter100.emit('message', { type : PATCH, at : hlc.from(_now), peer : 100, seq : 5, ver : 1, tab : 'testA', delta : { id : 5, tenantId : 1, name : '5a' } });
        _eventEmitter100.emit('message', { type : PATCH, at : hlc.from(_now-1), peer : 100, seq : 3, ver : 1, tab : 'testA', delta : { id : 5, tenantId : 1, name : '5c' } });

        setTimeout(() => {
          heartBeatSync(app);
          const _tableARows = db.prepare('SELECT * FROM testA').all();
          assert.deepStrictEqual(_tableARows, [{ id : 1, tenantId : 1, name : '1c' }, { id : 5, tenantId : 1, name : '5a' }]);

          assert.strictEqual(messageSentToPeer100.length, 4, 'Should have sent four messages to peer 100 for missing patch');
          assert.deepStrictEqual(simplifyStats(messageSentToPeer100)[2], { type : PING, at : _persistentPingPatchAt, peer : 1, seq : 1, ver : 1, tab : '_', delta : { '100' : [hlc.from(_now), 5, hlc.from(_now - 1), 3], '200' : [hlc.from(_now-1), 1, hlc.from(_now-1), 1] } });
          assert.deepStrictEqual(messageSentToPeer100[3], { type : MISSING_PATCH, peer : 100, minSeq : 4, maxSeq : 4, forPeer : 1 });

          _eventEmitter100.emit('message', { type : PATCH, at : hlc.from(_now), peer : 100, seq : 4, ver : 1, tab : 'testA', delta : { id : 5, tenantId : 1, name : '5z' } });
          setTimeout(() => {
            assert.deepStrictEqual(_syncedPeers, [200]);
            heartBeatSync(app);
            assert.deepStrictEqual(_syncedPeers, [200, 100]);
            const _tableARows = db.prepare('SELECT * FROM testA').all();
            assert.deepStrictEqual(_tableARows, [{ id : 1, tenantId : 1, name : '1c' }, { id : 5, tenantId : 1, name : '5a' }]);

            assert.strictEqual(messageSentToPeer100.length, 5, 'Should have sent five messages to peer 100 for missing patch');
            assert.deepStrictEqual(simplifyStats(messageSentToPeer100)[4], { type : PING, at : _persistentPingPatchAt, peer : 1, seq : 1, ver : 1, tab : '_', delta : { '100' : [hlc.from(_now), 5, hlc.from(_now), 4], '200' : [hlc.from(_now-1), 1, hlc.from(_now-1), 1] } });

            // Verify that _areAllMyPatchesStoredByPeers is updated correctly when receiving a ping with the statistics about me
            assert.strictEqual(app._areAllMyPatchesStoredByPeers(), false);
            // the peer 100 send a ping with the statistics about me (1)
            _eventEmitter100.emit('message', { type : PING, at : hlc.from(_now), peer : 100, seq : 4, ver : 1, delta : { '1' : [0, 0, 0, 1, 0, 0]}  });
            // But we are not synced yet, we must wait the ping of the other peer 200 to know that we are synced
            assert.strictEqual(app._areAllMyPatchesStoredByPeers(), false);
            _eventEmitter200.emit('message', { type : PING, at : hlc.from(_now), peer : 200, seq : 1, ver : 1, delta : { '1' : [0, 0, 0, 1, 0, 0]}  });
            assert.strictEqual(app._areAllMyPatchesStoredByPeers(), true);

            heartBeatSync(app);
            assert.deepStrictEqual(_syncedPeers, [200, 100]); // event "synced" should be fired only once per peer

            done();
          }, 15);
        }, 15);
      }, 15);
    });

  });


  describe('process existing patches', function () {
    let db, app;
    const existingPatchTimestamp = Date.now() - 1000; // 1 second ago
    const _eventEmitter100 = new EventEmitter();
    const _eventEmitter200 = new EventEmitter();
    let messageSentToPeer100 = [];
    let messageSentToPeer200 = [];
    beforeEach (function () {
      messageSentToPeer100 = [];
      messageSentToPeer200 = [];
      db = connect(); // memory db
      const _existingData = `
        INSERT INTO testA_patches (_patchedAt, _sequenceId, _peerId, id, tenantId, name)
        VALUES 
          (${hlc.from(existingPatchTimestamp - 400)}, 9,  100, 1, 10, 'existing20'),
          (${hlc.from(existingPatchTimestamp - 200)}, 10, 100, 2, 20, 'existing20'),
          (${hlc.from(existingPatchTimestamp - 100)}, 2 , 200, 3, 30, 'existing30'),
          (${hlc.from(existingPatchTimestamp - 300)}, 3 , 1  , 4, 40, 'existing40')
      `;
      app = SQLiteOnSteroid(db, 1, {  });
      app.migrate([{ up : _testSchema + _existingData, down : ''}]);
      _eventEmitter100.send = (message) => messageSentToPeer100.push(JSON.parse(JSON.stringify(message)));
      _eventEmitter200.send = (message) => messageSentToPeer200.push(JSON.parse(JSON.stringify(message)));
      const fakePeerSockets = {
        100 : _eventEmitter100,
        200 : _eventEmitter200
      };
      app.addRemotePeer(100, fakePeerSockets[100]);
      app.addRemotePeer(200, fakePeerSockets[200]);
    });
    afterEach (function (done) {
      setTimeout(() => {
        close(db); // let the job that write the patched table finished
        done();
      }, 20);
    });

    it('should trigger synced event only once per peer', function (done) {
      const _syncedPeers = [];
      app.event.on('synced', (peerId) => {
        _syncedPeers.push(peerId);
      });
      const _now = Date.now();
      const _status = app.status();
      assert.deepStrictEqual(_syncedPeers, []);
      assert.deepStrictEqual(_status.lastSequenceId, 3);
      assert.deepStrictEqual(_status.peerStats[100].slice(0,-2), [0,0,0,0]);
      assert.deepStrictEqual(_status.peerStats[200].slice(0,-2), [0,0,0,0]);
      assert.deepStrictEqual(messageSentToPeer100, []);
      assert.deepStrictEqual(messageSentToPeer200, []);
      setTimeout(() => {
        heartBeatSync(app);
        // should send a start patch to other peers
        assert.deepStrictEqual(messageSentToPeer100.length, 1);
        assert.deepStrictEqual(messageSentToPeer100[0].type, PATCH);
        assert.deepStrictEqual(messageSentToPeer100[0].peer, 1);
        assert.deepStrictEqual(messageSentToPeer100[0].seq, 4);
        assert.deepStrictEqual(_syncedPeers, []);
        // at startup, each send a persistent patch to make sure we are synced (could be improved in the feature)
        _eventEmitter100.emit('message', { type : PATCH, at : hlc.from(_now-1), peer : 100, seq : 3500, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 10, name : '1b' } });
        _eventEmitter200.emit('message', { type : PATCH, at : hlc.from(_now-1), peer : 200, seq : 10, ver : 1, tab : 'testA', delta : { id : 3, tenantId : 30, name : '1c' } });
        assert.deepStrictEqual(_syncedPeers, []);
        setTimeout(() => {
          heartBeatSync(app);
          // should send missing patch
          assert.deepStrictEqual(messageSentToPeer100.length, 3);
          assert.deepStrictEqual(messageSentToPeer100[2].type, MISSING_PATCH);
          assert.deepStrictEqual(messageSentToPeer100[2].peer, 100);
          assert.deepStrictEqual(messageSentToPeer100[2].minSeq, 11);
          assert.deepStrictEqual(messageSentToPeer100[2].maxSeq, 3499);

          assert.deepStrictEqual(messageSentToPeer200.length, 3);
          assert.deepStrictEqual(messageSentToPeer200[2].type, MISSING_PATCH);
          assert.deepStrictEqual(messageSentToPeer200[2].peer, 200);
          assert.deepStrictEqual(messageSentToPeer200[2].minSeq, 3);
          assert.deepStrictEqual(messageSentToPeer200[2].maxSeq, 9);
          // still not synced
          assert.deepStrictEqual(_syncedPeers, []);
          _eventEmitter100.emit('message', { type : PATCH, at : hlc.from(_now-1), peer : 100, seq : 3501, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 10, name : '1x' } });
          for (let i = 11; i < 1000; i++) {
            _eventEmitter100.emit('message', { type : PATCH, at : hlc.from(_now-1), peer : 100, seq : i, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 10, name : i+'-100' } });
          }
          for (let i = 7; i < 10; i++) {
            _eventEmitter200.emit('message', { type : PATCH, at : hlc.from(_now-1), peer : 200, seq : i, ver : 1, tab : 'testA', delta : { id : 3, tenantId : 30, name : i+'-200' } });
          }
          assert.deepStrictEqual(_syncedPeers, []);
          heartBeatSync(app);
          assert.deepStrictEqual(_syncedPeers, []);
          // still missing some patches
          assert.deepStrictEqual(messageSentToPeer100.length, 5);
          assert.deepStrictEqual(messageSentToPeer100[4].type, MISSING_PATCH);
          assert.deepStrictEqual(messageSentToPeer100[4].minSeq, 1000);
          assert.deepStrictEqual(messageSentToPeer100[4].maxSeq, 3499);
          // still missing some patches
          setTimeout(() => {
            heartBeatSync(app);
            assert.deepStrictEqual(_syncedPeers, []);
            for (let i = 1000; i < 3500; i++) {
              _eventEmitter100.emit('message', { type : PATCH, at : hlc.from(_now-1), peer : 100, seq : i, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 10, name : i+'-100' } });
            }
            assert.deepStrictEqual(_syncedPeers, []);
            heartBeatSync(app);
            setTimeout(() => {
              heartBeatSync(app);
              assert.deepStrictEqual(_syncedPeers, [100]);
              for (let i = 3; i < 7; i++) {
                _eventEmitter200.emit('message', { type : PATCH, at : hlc.from(_now-1), peer : 200, seq : i, ver : 1, tab : 'testA', delta : { id : 3, tenantId : 30, name : i+'-200' } });
              }
              heartBeatSync(app);
              assert.deepStrictEqual(_syncedPeers, [100, 200]);
              heartBeatSync(app);
              assert.deepStrictEqual(_syncedPeers, [100, 200]);
              // generate a new missing patch for peer 200
              assert.deepStrictEqual(messageSentToPeer200.length, 13);
              _eventEmitter100.emit('message', { type : PATCH, at : hlc.from(_now-1), peer : 200, seq : 12, ver : 1, tab : 'testA', delta : { id : 3, tenantId : 30, name : '12' } });
              heartBeatSync(app);
              assert.deepStrictEqual(messageSentToPeer200.length, 15);
              assert.deepStrictEqual(messageSentToPeer200[14].minSeq, 11);
              assert.deepStrictEqual(messageSentToPeer200[14].maxSeq, 11);
              _eventEmitter100.emit('message', { type : PATCH, at : hlc.from(_now-1), peer : 200, seq : 11, ver : 1, tab : 'testA', delta : { id : 3, tenantId : 30, name : '11' } });
              heartBeatSync(app);
              assert.deepStrictEqual(messageSentToPeer200.length, 16);
              assert.deepStrictEqual(messageSentToPeer200[15].type, PING);
              assert.deepStrictEqual(_syncedPeers, [100, 200]);
              done();
            }, 50);
          }, 50);
        }, 50);
      }, 50);
    });
  });

});


function heartBeatSync (app) {
  app._generatePingStatMessage();
  app._detectAndRequestMissingPatches();
}


function connect (filename = ':memory:') {
  // multi thread https://github.com/WiseLibs/better-sqlite3/blob/master/docs/performance.md
  // Follow better-sqlite3 API (fatser and same as Bun.js)
  const db = new Database(filename, {
    // verbose : console.log // TODO remove
  });
  // Enable WAL mode:
  // - significantly faster.
  // - provides more concurrency as readers do not block writers and a writer does not block readers. Reading and writing can proceed concurrently.
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = normal');
  // Return numbers (TODO convert to string constantId)
  db.defaultSafeIntegers(false);

  return db;
}

function close (db) {
  db?.close?.();
}