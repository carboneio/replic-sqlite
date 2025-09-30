const assert = require('assert');
const SQLiteOnSteroid = require('../lib/index.js');
const hlc = require('../lib/hlc.js');
const path = require('path');
const fs = require('fs');
const Database  = require('better-sqlite3');
const debug = require('debug');
const { simplifyStats, removeLastTimestampInStats } = require('./helper.js');
const MockDate = require('mockdate');

describe('main', function () {

  afterEach (function () {
    hlc._reset();
  });

  const _testSchema = `
    CREATE TABLE testA (
      id            INTEGER NOT NULL,
      tenantId      INTEGER NOT NULL,
      name          TEXT,
      deletedAt     INTEGER,
      createdAt     INTEGER,
      PRIMARY KEY (id, tenantId)
    ) STRICT;
    
    CREATE TABLE testA_patches (
      _patchedAt    INTEGER  NOT NULL, /* 53bits number (Unix timestamp in millisecond) -*/
      _sequenceId   INTEGER  NOT NULL, /* Consecutive sequence number of change per peer */
      _peerId       INTEGER  NOT NULL, /* 32bits globally unique, Source of change */
      
      id            INTEGER NOT NULL,
      tenantId      INTEGER NOT NULL,
      name          TEXT,
      deletedAt     INTEGER,
      createdAt     INTEGER
    ) STRICT;

    CREATE INDEX testA_patches_at_idx ON testA_patches (_patchedAt);
  `;
  describe('_generateMergePatchesQueryPlan', function () {
    let db, app;
    let _prepareStatementHookCalls = [];
    function prepareStatementHook (tableName, column) {
      _prepareStatementHookCalls.push({ tableName, column });
      return '?';
    }
    beforeEach (function () {
      db = connect(); // memory db
      app = SQLiteOnSteroid(db, 1, { prepareStatementHook });
      app.migrate([{ up : _testSchema, down : ''}]);
    });
    afterEach (function () {
      close(db);
    });
    it('should generate correct merge patches query plan', function () {
      _prepareStatementHookCalls = [];
      const _sql = app._generateMergePatchesQueryPlan('testA').applyPatchesSQL;
      const expectedSql = `
        INSERT INTO testA (id, tenantId, name, deletedAt, createdAt)
        SELECT
          id,
          tenantId,
          keep_last(name, _patchedAt, _peerId, _sequenceId),
          keep_last(deletedAt, _patchedAt, _peerId, _sequenceId),
          keep_last(createdAt, _patchedAt, _peerId, _sequenceId)
        FROM testA_patches
        WHERE _patchedAt >= ?
        GROUP BY id, tenantId
        ON CONFLICT (id, tenantId) DO UPDATE SET
          name = coalesce(excluded.name, name),
          deletedAt = coalesce(excluded.deletedAt, deletedAt),
          createdAt = coalesce(excluded.createdAt, createdAt);
        `;
      // Remove all whitespace and compare
      const normalizedSql = _sql.replace(/\s+/g, '\n');
      const normalizedExpectedSql = expectedSql.replace(/\s+/g, '\n');
      assert.strictEqual(normalizedSql, normalizedExpectedSql);
      assert.strictEqual(_prepareStatementHookCalls.length, 5);
      assert.strictEqual(_prepareStatementHookCalls[0].tableName, 'testA');
      assert.strictEqual(_prepareStatementHookCalls[0].column, 'id');
    });
    it('should generate correct insert patch query plan', function () {
      const _sql = app._generateMergePatchesQueryPlan('testA').savePatchSQL;
      const expectedSql = `
        INSERT INTO testA_patches (_patchedAt, _sequenceId, _peerId, id, tenantId, name, deletedAt, createdAt)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?);
      `;
      // Remove all whitespace and compare
      const normalizedSql = _sql.replace(/\s+/g, '\n');
      const normalizedExpectedSql = expectedSql.replace(/\s+/g, '\n');
      assert.strictEqual(normalizedSql, normalizedExpectedSql);
    });
    it('should generate correct direct upsert query plan', function () {
      const _sql = app._generateMergePatchesQueryPlan('testA').directUpsertSQL;
      const expectedSql = `
        INSERT INTO testA (id, tenantId, name, deletedAt, createdAt)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (id, tenantId) DO UPDATE SET
          name = coalesce(excluded.name, name),
          deletedAt = coalesce(excluded.deletedAt, deletedAt),
          createdAt = coalesce(excluded.createdAt, createdAt);
      `;
      // Remove all whitespace and compare
      const normalizedSql = _sql.replace(/\s+/g, '\n');
      const normalizedExpectedSql = expectedSql.replace(/\s+/g, '\n');
      assert.strictEqual(normalizedSql, normalizedExpectedSql);
    });
  });

  describe('upsert (Empty DB)', function () {
    let db, app;
    let fakePeerSockets;
    let broadcastedMessages;
    function createFakePeerSocket () {
      return {
        send : function (message) {
          broadcastedMessages.push(message);
        }
      };
    }
    beforeEach (function () {
      db = connect(); // memory db
      broadcastedMessages = [];
      fakePeerSockets = {
        100 : createFakePeerSocket(),
        101 : createFakePeerSocket(),
        102 : createFakePeerSocket()
      };
      app = SQLiteOnSteroid(db, 1800);
      app.addRemotePeer(100, fakePeerSockets[100], { ip : '127.0.0.1', port : 10000 });
      app.addRemotePeer(101, fakePeerSockets[101], { ip : '127.0.0.1', port : 10001 });
      app.addRemotePeer(102, fakePeerSockets[102], { ip : '127.0.0.1', port : 10002 });
      app.migrate([{ up : _testSchema, down : ''}]);
    });

    afterEach (function () {
      close(db);
    });

    it('should insert rows in patches and main tables and broadcast to peers. Should remove unknown columns before sending on the bus.', function (done) {
      const _rowPatch = { id : 1, tenantId : 2, name : 'test', deletedAt : 3, createdAt : 4, unknownColumn : 'unknownValue' };
      app.upsert('testA', _rowPatch, (err, sessionToken) => {
        if (err) {
          throw err;
        }
        assert.strictEqual(sessionToken, '1800.1');
        // Verify row in patches table
        const _patchRow = db.prepare('SELECT * FROM testA_patches').all();
        assert.ok(_patchRow.length === 1, 'Row should exist in patches table');
        assert.strictEqual(_patchRow[0].id, 1);
        assert.strictEqual(_patchRow[0].tenantId, 2);
        assert.strictEqual(_patchRow[0].name, 'test');
        assert.strictEqual(_patchRow[0].deletedAt, 3);
        assert.strictEqual(_patchRow[0].createdAt, 4);
        assert.ok(_patchRow[0]._patchedAt > 0);
        assert.strictEqual(_patchRow[0]._sequenceId, 1);
        assert.strictEqual(_patchRow[0]._peerId, 1800);

        // Verify row in main table
        const _mainRow = db.prepare('SELECT * FROM testA').all();
        assert.ok(_mainRow.length === 1, 'Row should exist in main table');
        assert.strictEqual(_mainRow[0].id, 1);
        assert.strictEqual(_mainRow[0].tenantId, 2);
        assert.strictEqual(_mainRow[0].name, 'test');
        assert.strictEqual(_mainRow[0].deletedAt, 3);
        assert.strictEqual(_mainRow[0].createdAt, 4);

        // Verify broadcast to peers
        assert.strictEqual(broadcastedMessages.length, 3, 'Patch should be broadcasted to all 3 peers');
        broadcastedMessages.forEach(broadcastedPatch => {
          assert.deepEqual(broadcastedPatch.delta, { id : 1, tenantId : 2, name : 'test', deletedAt : 3, createdAt : 4 }, 'Broadcasted patch should match the original rowPatch');
          assert.strictEqual(broadcastedPatch.tab, 'testA', 'Broadcasted patch should have correct table name');
          assert.ok(broadcastedPatch.at > 0, 'Broadcasted patch should have a timestamp');
          assert.ok(broadcastedPatch.seq > 0, 'Broadcasted patch should have a sequence number');
          assert.strictEqual(broadcastedPatch.ver, 1, 'Broadcasted patch should have correct version');
          assert.strictEqual(broadcastedPatch.peer, 1800, 'Broadcasted patch should have correct peer id');
        });

        done();
      });
    });

    it('should merge patches', async function () {
      const _rowPatch1 = { id : 1, tenantId : 2 , name : '2a' , deletedAt : 3, createdAt : 2   };
      const _rowPatch2 = { id : 1, tenantId : 2 , name : '2b' , deletedAt : 3, createdAt : 300 };
      const _rowPatch3 = { id : 1, tenantId : 2 , name : '2c'                                };
      const _rowPatch4 = { id : 2, tenantId : 30, name : '30a', deletedAt : 4, createdAt : 5 };
      const _rowPatch5 = { id : 2, tenantId : 30, name : '30b', deletedAt : 5, createdAt : 6 };
      const _rowPatch6 = { id : 2, tenantId : 30,               deletedAt : 7                };
      const _rowPatch7 = { id : 2, tenantId : 30,                              createdAt : 8 };
      const _rowPatches = [_rowPatch1, _rowPatch2, _rowPatch3, _rowPatch4, _rowPatch5, _rowPatch6, _rowPatch7];

      // Apply all patches sequentially
      for (const rowPatch of _rowPatches) {
        await new Promise((resolve, reject) => {
          app.upsert('testA', rowPatch, (err) => {
            if (err) {
              reject(err);
            }
            else {
              resolve();
            }
          });
        });
      }

      // Verify rows in patches table
      const _patchRows = db.prepare('SELECT * FROM testA_patches').all();
      assert.strictEqual(_patchRows.length, 7, 'All 7 patches should exist in patches table');

      // Verify sequence ID incrementation
      for (let i = 0; i < _patchRows.length; i++) {
        assert.strictEqual(_patchRows[i]._sequenceId, i + 1, `Sequence ID should be ${i + 1} for patch ${i + 1}`);
      }

      // Verify rows in main table
      const _mainRows = db.prepare('SELECT * FROM testA').all();
      assert.strictEqual(_mainRows.length, 2, 'Two rows should exist in main table');
      // Check the first row (id: 1, tenantId: 2)
      const _row1 = _mainRows.find(row => row.id === 1 && row.tenantId === 2);
      assert.strictEqual(_row1.name, '2c');
      assert.strictEqual(_row1.deletedAt, 3);
      assert.strictEqual(_row1.createdAt, 300);
      // Check the second row (id: 2, tenantId: 30)
      const _row2 = _mainRows.find(row => row.id === 2 && row.tenantId === 30);
      assert.strictEqual(_row2.name, '30b');
      assert.strictEqual(_row2.deletedAt, 7);
      assert.strictEqual(_row2.createdAt, 8);
    });
  });



  describe('upsert (With Existing Patches)', function () {
    let db, app;
    let fakePeerSockets;
    let broadcastedMessages;
    const existingPatchTimestamp = Date.now() - 1000; // 1 second ago

    function createFakePeerSocket () {
      return {
        send : function (message) {
          broadcastedMessages.push(message);
        }
      };
    }

    beforeEach (function () {
      db = connect(); // memory db
      broadcastedMessages = [];
      fakePeerSockets = {
        100 : createFakePeerSocket(),
        101 : createFakePeerSocket()
      };
      // Create schema first
      db.exec(_testSchema);
    });

    afterEach (function () {
      close(db);
    });

    it('should continue sequence from last sequenceId in patches table', function (done) {
      db.exec(`
        INSERT INTO testA_patches (_patchedAt, _sequenceId, _peerId, id, tenantId, name, deletedAt, createdAt)
        VALUES 
          (${hlc.from(existingPatchTimestamp)}, 1, 1800, 10, 20, 'existing1', 100, 200),
          (${hlc.from(existingPatchTimestamp + 100)}, 2, 1800, 11, 21, 'existing2', 101, 201)
      `);
      app = SQLiteOnSteroid(db, 1800);
      app.addRemotePeer(100, fakePeerSockets[100]);
      app.addRemotePeer(101, fakePeerSockets[101]);
      app.migrate([{ up : '', down : ''}]); // Empty migration since schema already exists

      // Insert a new patch
      const newRowPatch = { id : 12, tenantId : 22, name : 'new_patch', deletedAt : 102, createdAt : 202 };
      app.upsert('testA', newRowPatch, (err, sessionToken) => {
        if (err) {
          throw err;
        }
        assert.strictEqual(sessionToken, '1800.3');
        // Verify all patches including the new one
        const allPatches = db.prepare('SELECT * FROM testA_patches ORDER BY _sequenceId').all();
        assert.strictEqual(allPatches.length, 3, 'Should now have 3 patches');
        // Verify the new patch has sequenceId 3 (continuing from 2)
        const newPatch = allPatches[2];
        assert.strictEqual(newPatch._sequenceId, 3, 'New patch should have sequenceId 3');
        assert.strictEqual(newPatch.id, 12);
        assert.strictEqual(newPatch.tenantId, 22);
        assert.strictEqual(newPatch.name, 'new_patch');
        // Verify broadcast to peers has correct sequenceId
        assert.strictEqual(broadcastedMessages.length, 2, 'Patch should be broadcasted to all 2 peers');

        broadcastedMessages.forEach(broadcastedPatch => {
          assert.strictEqual(broadcastedPatch.seq, 3, 'Broadcasted patch should have a sequence number');
        });

        // Insert another patch to verify sequence continues
        const anotherRowPatch = { id : 13, tenantId : 23, name : 'another_patch', deletedAt : 103, createdAt : 203 };
        app.upsert('testA', anotherRowPatch, (err2, sessionToken) => {
          if (err2) {
            throw err2;
          }
          assert.strictEqual(sessionToken, '1800.4');
          const finalPatches = db.prepare('SELECT * FROM testA_patches ORDER BY _sequenceId').all();
          assert.strictEqual(finalPatches.length, 4, 'Should now have 4 patches');
          // Verify the newest patch has sequenceId 4
          const newestPatch = finalPatches[3];
          assert.strictEqual(newestPatch._sequenceId, 4, 'Newest patch should have sequenceId 4');
          // Check the last 3 broadcast messages (for the second patch)
          assert.strictEqual(broadcastedMessages.length, 4, 'Patch should be broadcasted to all 2 peers');
          const lastTwoBroadcasts = broadcastedMessages.slice(2, 4);
          lastTwoBroadcasts.forEach(broadcastedPatch => {
            assert.strictEqual(broadcastedPatch.seq, 4, 'Broadcasted patch should have a sequence number');
          });
          done();
        });
      });
    });

    it('should continue sequence from last sequenceId across all patch tables', function (done) {
      // Insert pre-existing patches in testA_patches
      db.exec(`
        INSERT INTO testA_patches (_patchedAt, _sequenceId, _peerId, id, tenantId, name, deletedAt, createdAt)
        VALUES 
          (${hlc.from(existingPatchTimestamp)}      , 1, 1800, 10, 20, 'existing1', 100, 200),
          (${hlc.from(existingPatchTimestamp + 200)}, 10, 100, 10, 20, 'existing1', 100, 200),
          (${hlc.from(existingPatchTimestamp + 100)}, 2, 1800, 11, 21, 'existing2', 101, 201)
      `);
      app = SQLiteOnSteroid(db, 1800);
      app.addRemotePeer(100, fakePeerSockets[100]);
      // Insert pre-existing patches in pending_patches with higher sequenceId, after migration of sqlite  on steroid
      const _preExistingPatch1 = `
        INSERT INTO pending_patches (_patchedAt, _peerId, _sequenceId, patchVersion, tableName, delta)
        VALUES 
          (${hlc.from(existingPatchTimestamp + 200)}, 1800, 3, 1, 'testA', jsonb('{"id":15,"tenantId":25,"name":"pending1","deletedAt":105,"createdAt":205}')),
          (${hlc.from(existingPatchTimestamp + 300)}, 1800, 4, 1, 'testA', jsonb('{"id":16,"tenantId":26,"name":"pending2","deletedAt":106,"createdAt":206}'))
      `;
      app.migrate([{ up : _preExistingPatch1, down : ''}]); // Empty table migration since schema already exists
      // Insert a new patch - should continue from sequenceId 4
      const newRowPatch = { id : 12, tenantId : 22, name : 'new_patch', deletedAt : 102, createdAt : 202 };
      app.upsert('testA', newRowPatch, (err, sessionToken) => {
        if (err) {
          throw err;
        }
        assert.strictEqual(sessionToken, '1800.5');
        // Verify all patches including the new one
        const allPatches = db.prepare('SELECT * FROM testA_patches ORDER BY _sequenceId').all();
        assert.strictEqual(allPatches.length, 4, 'Should now have 4 patches in testA_patches');
        // Verify the new patch has sequenceId 5 (continuing from 4 in pending_patches)
        const newPatch = allPatches[2];
        assert.strictEqual(newPatch._sequenceId, 5, 'New patch should have sequenceId 5');
        assert.strictEqual(newPatch.id, 12);
        assert.strictEqual(newPatch.tenantId, 22);
        assert.strictEqual(newPatch.name, 'new_patch');
        // Verify broadcast to peers has correct sequenceId
        assert.strictEqual(broadcastedMessages.length, 1, 'Patch should be broadcasted to all 2 peers');
        assert.strictEqual(broadcastedMessages[0].seq, 5, 'Broadcasted patch should have sequence number 5');
        done();
      });
    });
  });

  describe('deleteOldPatches', function () {
    let db, app;
    beforeEach (function () {
      db = connect(); // memory db
      db.exec(_testSchema);
    });
    afterEach (function () {
      close(db);
    });
    it('should delete old patches', function (done) {
      const existingPatchTimestamp = Date.now(); // 1 second ago
      // Insert pre-existing patches in testA_patches
      db.exec(`
        INSERT INTO testA_patches (_patchedAt, _sequenceId, _peerId, id, tenantId, name, deletedAt, createdAt)
        VALUES 
          (${hlc.from(existingPatchTimestamp)}      , 1, 1800, 10, 20, 'existing1', 100, 200),
          (${hlc.from(existingPatchTimestamp - 500)}, 10, 100, 10, 20, 'existing1', 100, 200),
          (${hlc.from(existingPatchTimestamp - 600)}, 2, 1800, 11, 21, 'existing2', 101, 201)
      `);
      app = SQLiteOnSteroid(db, 1800, { maxPatchRetentionMs : 100 });
      // Insert pre-existing patches in pending_patches with higher sequenceId, after migration of sqlite  on steroid
      const _preExistingPatch1 = `
        INSERT INTO pending_patches (_patchedAt, _peerId, _sequenceId, patchVersion, tableName, delta)
        VALUES 
          (${hlc.from(existingPatchTimestamp - 50)}, 1800, 3, 1, 'testA', jsonb('{"id":15,"tenantId":25,"name":"pending1","deletedAt":105,"createdAt":205}')),
          (${hlc.from(existingPatchTimestamp - 300)}, 1800, 4, 1, 'testA', jsonb('{"id":16,"tenantId":26,"name":"pending2","deletedAt":106,"createdAt":206}'))
      `;
      app.migrate([{ up : _preExistingPatch1, down : ''}]);
      // Delete old patches
      app._deleteOldPatches();
      // Verify all patches including the new one
      const _allPatches = db.prepare('SELECT * FROM testA_patches ORDER BY _sequenceId').all();
      assert.strictEqual(_allPatches.length, 1, 'Should now have 1 patch in testA_patches');
      assert.strictEqual(hlc.toUnixTimestamp(_allPatches[0]._patchedAt), existingPatchTimestamp);

      const _allPendingPatches = db.prepare('SELECT * FROM pending_patches ORDER BY _sequenceId').all();
      assert.strictEqual(_allPendingPatches.length, 1, 'Should now have 1 patch in pending_patches');
      assert.strictEqual(hlc.toUnixTimestamp(_allPendingPatches[0]._patchedAt), existingPatchTimestamp - 50);
      done();
    });
  });

  describe('_onPatchReceivedFromPeers', function () {
    let db, app;
    const patchApplyDelayMs = 20;
    beforeEach (function () {
      db = connect(); // memory db
      app = SQLiteOnSteroid(db, 1, { patchApplyDelayMs : (patchApplyDelayMs - 10)  });
      app.migrate([{ up : _testSchema, down : ''}]);
    });
    afterEach (function () {
      close(db);
    });

    it('should not crash if the table is unknown when receiving a patch', function (done) {
      const baseTimestamp = Date.now();
      const _rowPatch1 = { at : baseTimestamp + 1, peer : 20, seq : 1, ver : 1, tab : 'UnknwonwtestA', delta : { id : 1, tenantId : 2 , name : '2a' , deletedAt : 3, createdAt : 2   } };
      app._onPatchReceivedFromPeers(_rowPatch1);
      done();
    });

    it('should merge patches coming from peers. Should ignore patch coming from myself. ', function (done) {
      const baseTimestamp = Date.now();
      const _rowPatch1 = { at : hlc.from(baseTimestamp + 1), peer : 20, seq : 1, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2 , name : '2a' , deletedAt : 3, createdAt : 2   } };
      const _rowPatch2 = { at : hlc.from(baseTimestamp + 2), peer : 20, seq : 2, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2 , name : '2b' , deletedAt : 3, createdAt : 300 } };
      const _rowPatch3 = { at : hlc.from(baseTimestamp + 3), peer : 20, seq : 3, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2 , name : '2c'                                } };
      const _rowPatch4 = { at : hlc.from(baseTimestamp + 4), peer : 20, seq : 4, ver : 1, tab : 'testA', delta : { id : 2, tenantId : 30, name : '30a', deletedAt : 4, createdAt : 5 } };
      const _rowPatch5 = { at : hlc.from(baseTimestamp + 5), peer : 20, seq : 5, ver : 1, tab : 'testA', delta : { id : 2, tenantId : 30, name : '30b', deletedAt : 5, createdAt : 6 } };
      const _rowPatch6 = { at : hlc.from(baseTimestamp + 6), peer : 20, seq : 6, ver : 1, tab : 'testA', delta : { id : 2, tenantId : 30,               deletedAt : 7                } };
      const _rowPatch7 = { at : hlc.from(baseTimestamp + 7), peer : 20, seq : 7, ver : 1, tab : 'testA', delta : { id : 2, tenantId : 30,                              createdAt : 8 } };
      const _rowPatch8 = { at : hlc.from(baseTimestamp + 8), peer : 1 , seq : 7, ver : 1, tab : 'testA', delta : { id : 2, tenantId : 30,                              createdAt : 9 } };
      const _patches = [_rowPatch1, _rowPatch2, _rowPatch3, _rowPatch4, _rowPatch5, _rowPatch6, _rowPatch7, _rowPatch8];

      // Apply all patches sequentially
      for (const _patch of _patches) {
        app._onPatchReceivedFromPeers(_patch);
      }

      // Verify rows in patches table
      const _patchRows = db.prepare('SELECT * FROM testA_patches').all();
      assert.strictEqual(_patchRows.length, 7, 'All 7 patches should exist in patches table');

      // Verify rows in main table
      setTimeout(() => {
        const _mainRows = db.prepare('SELECT * FROM testA').all();
        assert.strictEqual(_mainRows.length, 2, 'Two rows should exist in main table');
        // Check the first row (id: 1, tenantId: 2)
        const _row1 = _mainRows.find(row => row.id === 1 && row.tenantId === 2);
        assert.strictEqual(_row1.name, '2c');
        assert.strictEqual(_row1.deletedAt, 3);
        assert.strictEqual(_row1.createdAt, 300);
        // Check the second row (id: 2, tenantId: 30)
        const _row2 = _mainRows.find(row => row.id === 2 && row.tenantId === 30);
        assert.strictEqual(_row2.name, '30b');
        assert.strictEqual(_row2.deletedAt, 7);
        assert.strictEqual(_row2.createdAt, 8);
        done();
      }, patchApplyDelayMs);
    });

    it('should merge patches coming, sort patch by date, then by peer id, then by sequence id', function (done) {
      const baseTimestamp = Date.now();
      const _rowPatch1 = { at : hlc.from(baseTimestamp + 1), peer : 20, seq : 1, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2, name : '2a' } };
      const _rowPatch2 = { at : hlc.from(baseTimestamp + 1), peer : 21, seq : 1, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2, name : '2b' } };
      const _rowPatch3 = { at : hlc.from(baseTimestamp + 1), peer : 21, seq : 2, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2, name : '2c' } };
      const _rowPatch4 = { at : hlc.from(baseTimestamp + 2), peer : 19, seq : 1, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2, name : '2d' } };
      const _rowPatch5 = { at : hlc.from(baseTimestamp + 2), peer : 20, seq : 1, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2, name : '2e' } };
      const _rowPatch6 = { at : hlc.from(baseTimestamp + 3), peer : 18, seq : 1, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2, name : '2f' } };
      const _rowPatch7 = { at : hlc.from(baseTimestamp + 3), peer : 18, seq : 2, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2, name : '2g' } };
      const _patches = [_rowPatch7, _rowPatch3, _rowPatch1, _rowPatch6, _rowPatch2, _rowPatch5, _rowPatch4];
      // Apply all patches sequentially
      for (const _patch of _patches) {
        app._onPatchReceivedFromPeers(_patch);
      }

      // Verify rows in patches table
      const _patchRows = db.prepare('SELECT * FROM testA_patches').all();
      assert.strictEqual(_patchRows.length, 7, 'All 7 patches should exist in patches table');

      // Verify rows in main table
      setTimeout(() => {
        const _mainRows = db.prepare('SELECT * FROM testA').all();
        assert.strictEqual(_mainRows.length, 1, 'One row should exist in main table');
        // Check the first row (id: 1, tenantId: 2)
        const _row1 = _mainRows.find(row => row.id === 1 && row.tenantId === 2);
        assert.strictEqual(_row1.name, '2g');
        done();
      }, patchApplyDelayMs);
    });

    it('should merge patches coming other peer, and should create new patch with a timestamp than last patch receive from peers even if there is a time skew', function (done) {
      MockDate.set(1759276800000); // 1759276800000
      const baseTimestamp = Date.now();
      const _rowPatch1 = { at : hlc.from(baseTimestamp + 1), peer : 20, seq : 1, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2, name : '2a' } };
      const _rowPatch2 = { at : hlc.from(baseTimestamp + 0), peer : 21, seq : 1, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2, name : '2b' } };
      const _rowPatch3 = { at : hlc.from(baseTimestamp + 1), peer : 21, seq : 2, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2, name : '2c' } };
      const _rowPatch7 = { at : hlc.from(baseTimestamp + 1), peer : 18, seq : 2, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2, name : '2g' } };
      const _patches = [_rowPatch7, _rowPatch3, _rowPatch1, _rowPatch2];
      for (const _patch of _patches) {
        app._onPatchReceivedFromPeers(_patch);
      }
      // Set the time of the node in the past to simulate a time skew
      MockDate.set(1759276800000-100); // 1759276800000
      // This patch is created after the reception of other peer's patches.
      // Even if the Node leaves in the past (clock not synchronized), it should create a new patch with a timestamp greater than the last patch received from peers.
      app.upsert('testA', { id : 1, tenantId : 2, name : '2d' }, (err) => {
        if (err) {
          throw err;
        }
        // Verify rows in patches table
        const _patchRows = db.prepare('SELECT * FROM testA_patches ORDER BY _peerId').all();
        assert.strictEqual(_patchRows.length, 5, 'All 5 patches should exist in patches table');
        assert.strictEqual(_patchRows[0]._peerId, 1);
        assert.strictEqual(hlc.toUnixTimestamp(_patchRows[0]._patchedAt), baseTimestamp+1);
        assert.strictEqual(hlc.toCounter(_patchRows[0]._patchedAt), 1);

        // Verify rows in main table
        setTimeout(() => {
          const _mainRows = db.prepare('SELECT * FROM testA').all();
          assert.strictEqual(_mainRows.length, 1, 'One row should exist in main table');
          const _row1 = _mainRows.find(row => row.id === 1 && row.tenantId === 2);
          assert.strictEqual(_row1.name, '2d');
          MockDate.reset();
          done();
        }, patchApplyDelayMs);
      });
    });

    it('should merge patches with Date.now() timestamp, sorting by peer id and sequence id', function (done) {
      const _constantTimestamp = Date.now(); // A fixed timestamp for all patches
      const _rowPatch1 = { at : hlc.from(_constantTimestamp), peer : 20, seq : 1, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2, name : '2a' } };
      const _rowPatch2 = { at : hlc.from(_constantTimestamp), peer : 20, seq : 2, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2, name : '2b' } };
      const _rowPatch3 = { at : hlc.from(_constantTimestamp), peer : 19, seq : 1, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2, name : '2c' } };
      const _rowPatch4 = { at : hlc.from(_constantTimestamp), peer : 19, seq : 2, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2, name : '2d' } };
      const _rowPatch5 = { at : hlc.from(_constantTimestamp), peer : 21, seq : 1, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2, name : '2e' } };
      const _rowPatch6 = { at : hlc.from(_constantTimestamp), peer : 21, seq : 2, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2, name : '2f' } };
      const _patches = [_rowPatch3, _rowPatch6, _rowPatch1, _rowPatch5, _rowPatch2, _rowPatch4];

      // Apply all patches sequentially
      for (const _patch of _patches) {
        app._onPatchReceivedFromPeers(_patch);
      }

      // Verify rows in patches table
      const _patchRows = db.prepare('SELECT * FROM testA_patches').all();
      assert.strictEqual(_patchRows.length, 6, 'All 6 patches should exist in patches table');

      // Verify rows in main table
      setTimeout(() => {
        const _mainRows = db.prepare('SELECT * FROM testA').all();
        assert.strictEqual(_mainRows.length, 1, 'One row should exist in main table');
        // Check the first row (id: 1, tenantId: 2)
        const _row1 = _mainRows.find(row => row.id === 1 && row.tenantId === 2);
        assert.strictEqual(_row1.name, '2f', 'The last applied patch should be from peer 21, seq 2');
        done();
      }, patchApplyDelayMs);
    });

    it('should store stats in pending_patches table', function (done) {
      const _constantTimestamp = Date.now(); // A fixed timestamp for all patches
      const _rowPatch1 = { at : hlc.from(_constantTimestamp), peer : 20, seq : 1 , ver : 1, tab : '_', delta : { 20 : [0,0] } };
      const _rowPatch2 = { at : hlc.from(_constantTimestamp), peer : 10, seq : 20, ver : 2, tab : '_', delta : { 20 : [1,1] } };
      const _patches = [_rowPatch1, _rowPatch2];

      // Apply all patches sequentially
      for (const _patch of _patches) {
        app._onPatchReceivedFromPeers(_patch);
      }

      const _patchRows = db.prepare('SELECT *, json(delta) as delta FROM pending_patches').all();
      assert.strictEqual(_patchRows.length, 2, 'All 2 patches should exist in pending_patches table');
      assert.deepStrictEqual(_patchRows, [{
        _patchedAt   : hlc.from(_constantTimestamp),
        _peerId      : 20,
        _sequenceId  : 1,
        delta        : '{\"20\":[0,0]}',
        patchVersion : 1,
        tableName    : '_'
      }, {
        _patchedAt   : hlc.from(_constantTimestamp),
        _peerId      : 10,
        _sequenceId  : 20,
        delta        : '{\"20\":[1,1]}',
        patchVersion : 2,
        tableName    : '_'
      }]);

      done();
    });

    it('should save patches with mismatching version to pending_patches table and apply them later', function (done) {
      const _constantTimestamp = Date.now();
      const _rowPatch1 = { at : hlc.from(_constantTimestamp), peer : 20, seq : 1, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 2, name : '2a' } };
      const _rowPatch2 = { at : hlc.from(_constantTimestamp), peer : 21, seq : 2, ver : 2, tab : 'testA', delta : { id : 2, tenantId : 3, name : '3a' } };
      const _rowPatch3 = { at : hlc.from(_constantTimestamp), peer : 22, seq : 3, ver : 3, tab : 'testA', delta : { id : 3, tenantId : 4, name : '4a' } };

      // Apply all patches
      app._onPatchReceivedFromPeers(_rowPatch1);
      app._onPatchReceivedFromPeers(_rowPatch2);
      app._onPatchReceivedFromPeers(_rowPatch3);

      setTimeout(() => {
        // Verify rows in patches table
        const _patchRows = db.prepare('SELECT * FROM testA_patches').all();
        assert.strictEqual(_patchRows.length, 1, 'Only 1 patch should exist in patches table');

        // Verify rows in pending_patches table
        const _pendingPatchRows = db.prepare('SELECT patchVersion, _sequenceId, _peerId, tableName, json(delta) as delta FROM pending_patches').all();
        assert.strictEqual(_pendingPatchRows.length, 2, '2 patches should exist in pending_patches table');

        // Verify content of pending patches
        const _pendingPatch1 = _pendingPatchRows.find(row => row._peerId === 21);
        assert.strictEqual(_pendingPatch1.patchVersion, 2);
        assert.strictEqual(_pendingPatch1._sequenceId, 2);
        assert.strictEqual(_pendingPatch1.tableName, 'testA');
        assert.deepStrictEqual(JSON.parse(_pendingPatch1.delta), { id : 2, tenantId : 3, name : '3a' });

        const _pendingPatch2 = _pendingPatchRows.find(row => row._peerId === 22);
        assert.strictEqual(_pendingPatch2.patchVersion, 3);
        assert.strictEqual(_pendingPatch2._sequenceId, 3);
        assert.strictEqual(_pendingPatch2.tableName, 'testA');
        assert.deepStrictEqual(JSON.parse(_pendingPatch2.delta), { id : 3, tenantId : 4, name : '4a' });

        done();
      }, patchApplyDelayMs);
    });

    it('should process 100000 patches within a reasonable time', function (done) {
      const _numPatches = 100000;
      const _threshold = 500;
      const _patches = [];

      // Create patches
      for (let i = 0; i < _numPatches; i++) {
        _patches.push({
          at    : hlc.from(Date.now()),
          peer  : Math.floor(Math.random() * 5)+2,
          seq   : i + 1,
          ver   : 1,
          tab   : 'testA',
          delta : { id : i + 1, tenantId : Math.floor(Math.random() * 10), name : `name${i}` }
        });
      }

      // Measure performance of _onPatchReceivedFromPeers
      const _startTime = process.hrtime();
      for (const _patch of _patches) {
        app._onPatchReceivedFromPeers(_patch);
      }
      const [_seconds, _nanoseconds] = process.hrtime(_startTime);
      const _elapsedTime = (_seconds * 1000) + (_nanoseconds / 1000000);

      console.log(`Time taken to process ${_numPatches} patches: ${_elapsedTime.toFixed(2)}ms (190 ms)`);

      setTimeout(() => {
        assert.strictEqual(db.prepare('SELECT COUNT(*) as count FROM testA_patches').get().count, _numPatches, `All ${_numPatches} patches should exist in patches table`);
        assert(_elapsedTime < _threshold, `Processing time (${_elapsedTime}ms) exceeded the threshold of ${_threshold}ms`);
        done();
      }, patchApplyDelayMs);
    });
  });

  describe('_generatePingStatMessage', function () {
    let db, app;
    const patchApplyDelayMs = 20;
    let messageSentToPeer2 = [];
    let messageSentToPeer10 = [];
    beforeEach (function () {
      messageSentToPeer2 = [];
      messageSentToPeer10 = [];
      db = connect(); // memory db
      app = SQLiteOnSteroid(db, 1, { patchApplyDelayMs : (patchApplyDelayMs - 10)  });
      app.migrate([{ up : _testSchema, down : ''}]);
      const fakePeerSockets = {
        10 : { send : (message) => messageSentToPeer10.push(message) },
        2  : { send : (message) => messageSentToPeer2.push(message) }
      };
      app.addRemotePeer(10, fakePeerSockets[10]);
      app.addRemotePeer(2, fakePeerSockets[2]);
    });
    afterEach (function () {
      close(db);
    });

    it('should store stats in pending_patches table', function (done) {
      app._generatePingStatMessage(true);
      const _patchRows = db.prepare('SELECT *, json(delta) as delta FROM pending_patches').all();
      assert(Math.abs(hlc.toUnixTimestamp(_patchRows[0]._patchedAt) - Date.now()) < 1000, '_patchedAt should be within 1 second of current time');
      assert.deepStrictEqual(_patchRows, [{
        _patchedAt   : _patchRows[0]._patchedAt,
        _peerId      : 1,
        _sequenceId  : 1,
        delta        : JSON.stringify({ 2 : [0, 0, 0, 0, 0], 10 : [0, 0, 0, 0, 0] }),
        patchVersion : 1,
        tableName    : '_'
      }]);
      assert.strictEqual(_patchRows.length, 1, 'All 1 patches should exist in pending_patches table');
      done();
    });

    it('should send non persistent peer stat message to all peers and it should not increment the sequence id', function (done) {
      app._generatePingStatMessage(false);
      const _patchRows = db.prepare('SELECT * FROM pending_patches').all();
      assert.equal(_patchRows.length, 0);
      assert.deepStrictEqual(messageSentToPeer10, [{ type : 20 /* PEER_STATS */, at : 0, peer : 1, seq : 0, ver : 1, tab : '_', delta : { 2 : [0, 0, 0, 0, 0], 10 : [0, 0, 0, 0, 0] } }]);
      assert.deepStrictEqual(messageSentToPeer2, [{ type : 20 /* PEER_STATS */ , at : 0, peer : 1, seq : 0, ver : 1, tab : '_', delta : { 2 : [0, 0, 0, 0, 0], 10 : [0, 0, 0, 0, 0] } }]);
      // create a persistent ping messgae
      app._generatePingStatMessage(true);
      assert(Math.abs(hlc.toUnixTimestamp(messageSentToPeer10[1].at) - Date.now()) < 500, 'Timestamps should be within 500ms of each other');
      assert.equal(messageSentToPeer10[1].seq, 1);
      // verify that the next non persistent ping stat message is sent with the same sequence id and timestamp
      setTimeout(() => {
        app._generatePingStatMessage(false);
        assert.equal(messageSentToPeer10[2].seq, 1);
        assert.equal(messageSentToPeer10[2].at, messageSentToPeer10[1].at);
        done();
      }, 100);
    });
  });


  describe('_getMissingPatches', function () {
    let db, app;
    const patchApplyDelayMs = 20;
    let fakePeerSockets = {};
    let broadcastedMessages = [];
    function createFakePeerSocket () {
      return {
        send : function (message) {
          broadcastedMessages.push(message);
        }
      };
    }
    beforeEach (function () {
      db = connect(); // memory db
      broadcastedMessages = [];
      fakePeerSockets = {
        10 : createFakePeerSocket(),
        2  : createFakePeerSocket()
      };
    });
    afterEach (function () {
      close(db);
    });

    // TODO
    it.skip('limit : 1');

    it('should detect missing sequence IDs (excluding my own patches, peerId = 1) and notify peers, and read in all tables (pending tables) and return them in order by peerId and sequenceId, and update peer stats', function (done) {
      app = SQLiteOnSteroid(db, 1);
      app.addRemotePeer(10, fakePeerSockets[10]);
      app.addRemotePeer(2, fakePeerSockets[2]);
      app.addRemotePeer(3, fakePeerSockets[3]);
      app.migrate([{ up : _testSchema, down : ''}]);

      const _patches = [
        { at : hlc.from(1748505463330), peer : 1 , seq : 1 , ver : 1, tab : 'testA', delta : { id : 1, tenantId : 1, name : '1a' } },
        { at : hlc.from(1748505463330), peer : 1 , seq : 3 , ver : 1, tab : 'testA', delta : { id : 3, tenantId : 1, name : '3a' } },
        { at : hlc.from(1748505463330), peer : 1 , seq : 5 , ver : 1, tab : 'testA', delta : { id : 5, tenantId : 1, name : '5a' } },
        { at : hlc.from(1748505463335), peer : 10, seq : 1 , ver : 1, tab : 'testA', delta : { id : 1, tenantId : 1, name : '1a' } },
        { at : hlc.from(1748505463336), peer : 10, seq : 3 , ver : 1, tab : 'testA', delta : { id : 3, tenantId : 1, name : '3a' } },
        { at : hlc.from(1748505463337), peer : 10, seq : 5 , ver : 1, tab : 'testA', delta : { id : 5, tenantId : 1, name : '5a' } },
        { at : hlc.from(1748505463338), peer : 2 , seq : 1 , ver : 1, tab : 'testA', delta : { id : 6, tenantId : 2, name : '6a' } },
        { at : hlc.from(1748505463340), peer : 2 , seq : 4 , ver : 1, tab : 'testA', delta : { id : 9, tenantId : 2, name : '9a' } },
        { at : hlc.from(1748505463339), peer : 2 , seq : 2 , ver : 1, tab : 'testA', delta : { id : 7, tenantId : 2, name : '7a' } },
        { at : hlc.from(1748505463341), peer : 2 , seq : 10, ver : 2, tab : 'testB', delta : { id : 10, tenantId : 2, name : '10a' }},
        { at : hlc.from(1748505463342), peer : 2 , seq : 15, ver : 2, tab : 'testB', delta : { id : 15, tenantId : 2, name : '15a' }},
        { at : hlc.from(1748505463342), peer : 3 , seq : 1 , ver : 1, tab : 'testA', delta : { id : 1 , tenantId : 3, name : '30a' }},
        { at : hlc.from(1748505463342), peer : 3 , seq : 2 , ver : 1, tab : 'testA', delta : { id : 2 , tenantId : 3, name : '30a' }},
      ];

      // Apply all patches
      for (const patch of _patches) {
        app._onPatchReceivedFromPeers(patch);
      }

      setTimeout(() => {
        assert.deepStrictEqual(removeLastTimestampInStats(app.status().peerStats), {
          2  : [hlc.from(1748505463342), 15, hlc.from(1748505463339), 2],
          3  : [hlc.from(1748505463342), 2 , hlc.from(1748505463342), 2],
          10 : [hlc.from(1748505463337), 5 , hlc.from(1748505463335), 1]
        });
        const { peerStats } = app.status();
        // Check that the last message timestamp for peer 2 is approximately now
        const _lastMsgTimestamp = peerStats['2'][app.LAST_MESSAGE_TIMESTAMP];
        // Should be within 200ms of now (allowing for test delays)
        assert.ok(Math.abs(_lastMsgTimestamp - Date.now()) < 2000, `Expected lastMsgTimestamp ~ now, got ${_lastMsgTimestamp}`);
        // Simulate fake missing patches for peer 3 to verify that _getMissingPatches fixes the stats
        peerStats['3'][app.GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP] = hlc.from(1748505463000);
        peerStats['3'][app.GUARANTEED_CONTIGUOUS_SEQUENCE_ID] = 0;
        assert.deepStrictEqual(removeLastTimestampInStats(app.status().peerStats)['3'], [hlc.from(1748505463342), 2 , hlc.from(1748505463000), 0]);
        // same for peer 2
        peerStats['2'][app.GUARANTEED_CONTIGUOUS_PATCH_AT_TIMESTAMP] = hlc.from(1748505463338);
        peerStats['2'][app.GUARANTEED_CONTIGUOUS_SEQUENCE_ID] = 1;

        // Get missing patches from beginning of time
        const _missingSequenceIds = app._getMissingPatches(0);
        // Should find missing sequence IDs 2 and 4 for peer 1, and 3 for peer 2
        assert.strictEqual(_missingSequenceIds.length, 5);
        assert.deepStrictEqual(
          _missingSequenceIds,
          [
            { peerId : 2 , sequenceId : 2 , patchedAt : hlc.from(1748505463339), nbMissingSequenceIds : 1},
            { peerId : 2 , sequenceId : 4 , patchedAt : hlc.from(1748505463340), nbMissingSequenceIds : 5},
            { peerId : 2 , sequenceId : 10, patchedAt : hlc.from(1748505463341), nbMissingSequenceIds : 4},
            { peerId : 10, sequenceId : 1 , patchedAt : hlc.from(1748505463335), nbMissingSequenceIds : 1},
            { peerId : 10, sequenceId : 3 , patchedAt : hlc.from(1748505463336), nbMissingSequenceIds : 1}
          ]
        );

        // Verify that peer sockets were called to request missing patches
        assert.strictEqual(broadcastedMessages.length, 5, 'Should have sent messages to request missing patches');

        // Verify the content of each broadcasted message
        assert.deepStrictEqual(broadcastedMessages, [
          { type : 30 /* MISSING_PATCH */, peer : 2 , minSeq : 3 , maxSeq : 3 , forPeer : 1 },
          { type : 30 /* MISSING_PATCH */, peer : 2 , minSeq : 5 , maxSeq : 9 , forPeer : 1 },
          { type : 30 /* MISSING_PATCH */, peer : 2 , minSeq : 11, maxSeq : 14, forPeer : 1 },
          { type : 30 /* MISSING_PATCH */, peer : 10, minSeq : 2 , maxSeq : 2 , forPeer : 1 },
          { type : 30 /* MISSING_PATCH */, peer : 10, minSeq : 4 , maxSeq : 4 , forPeer : 1 }
        ]);

        // Verify that all stats are up to date
        assert.deepStrictEqual(removeLastTimestampInStats(app.status().peerStats), {
          2  : [hlc.from(1748505463342), 15, hlc.from(1748505463339), 2],
          3  : [hlc.from(1748505463342), 2 , hlc.from(1748505463342), 2],
          10 : [hlc.from(1748505463337), 5 , hlc.from(1748505463335), 1]
        });

        done();
      }, patchApplyDelayMs);
    });

    it('should efficiently detect missing sequence IDs with a large number of patches, and a large number of missing sequence IDs. Should not crash if there is no corresponding sockets', function (done) {
      app = SQLiteOnSteroid(db, 1);
      app.addRemotePeer(2, fakePeerSockets[2]);
      app.addRemotePeer(3, fakePeerSockets[3]);
      app.addRemotePeer(4, fakePeerSockets[4]);
      app.addRemotePeer(5, fakePeerSockets[5]);
      app.migrate([{ up : _testSchema, down : ''}]);

      const _numPatches = 100000;
      const _threshold = 100; // ms
      const _patches = [];
      const _peerCount = 10;
      // Create patches
      for (let i = 0; i < _numPatches; i++) {
        const peerId = Math.floor(Math.random() * _peerCount) + 2;
        _patches.push({
          at    : Date.now(),
          peer  : peerId,
          seq   : Math.floor(i / _peerCount) + 1, // Ensure some missing sequence IDs
          ver   : 1,
          tab   : 'testA',
          delta : { id : i + 1, tenantId : peerId, name : `name${i}` }
        });
      }
      // Apply all patches
      for (const patch of _patches) {
        app._onPatchReceivedFromPeers(patch);
      }
      setTimeout(() => {
        // Measure performance of _getMissingPatches
        const _startTime = process.hrtime();
        const _missingSequenceIds = app._getMissingPatches(0);
        const [_seconds, _nanoseconds] = process.hrtime(_startTime);
        const _elapsedTime = _seconds * 1000 + _nanoseconds / 1000000;

        console.log(`Time taken to find missing sequence IDs among ${_numPatches} patches: ${_elapsedTime.toFixed(2)}ms (55ms)`);
        assert(_elapsedTime < _threshold, `Processing time (${_elapsedTime}ms) exceeded the threshold of ${_threshold}ms`);
        // Verify that we found some missing sequence IDs
        assert(_missingSequenceIds.length > 0, 'Should have found some missing sequence IDs');
        // assert(_missingSequenceIds.length <= _peerCount, 'Should not return more missing sequence IDs than the number of peers of there are two may missing patches');

        done();
      }, patchApplyDelayMs);
    });


  });

  describe('_onRequestForMissingPatchFromPeers', function () {
    let db, app;
    const patchApplyDelayMs = 20;
    let fakePeerSockets = {};
    let messagesPerPeer = {};
    function createFakePeerSocket (peerId) {
      return {
        send : function (message) {
          messagesPerPeer[peerId].push(message);
        }
      };
    }
    beforeEach (function () {
      db = connect(); // memory db
      messagesPerPeer = {
        10 : [],
        2  : []
      };
      fakePeerSockets = {
        10 : createFakePeerSocket(10),
        2  : createFakePeerSocket(2)
      };
    });
    afterEach (function () {
      close(db);
    });


    it('should return the missing patch to the right peer', function (done) {
      app = SQLiteOnSteroid(db, 1);
      app.addRemotePeer(10, fakePeerSockets[10]);
      app.addRemotePeer(2, fakePeerSockets[2]);
      app.migrate([{ up : _testSchema, down : ''}]);
      const _constantTimestamp = Date.now();
      const _patches = [
        { at : hlc.from(_constantTimestamp), peer : 3 , seq : 1, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 1, name : '1a' } },
        { at : hlc.from(_constantTimestamp), peer : 3 , seq : 3, ver : 2, tab : 'testB', delta : { id : 3, tenantId : 1, name : '3a' } }, // missing B
        { at : hlc.from(_constantTimestamp), peer : 3 , seq : 5, ver : 1, tab : 'testA', delta : { id : 5, tenantId : 1, name : '5a' } },
        { at : hlc.from(_constantTimestamp), peer : 10, seq : 1, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 1, name : '1a' } },
        { at : hlc.from(_constantTimestamp), peer : 10, seq : 3, ver : 2, tab : 'testA', delta : { id : 3, tenantId : 1, name : '3a' } },
        { at : hlc.from(_constantTimestamp), peer : 10, seq : 5, ver : 1, tab : 'testA', delta : { id : 5, tenantId : 1, name : '5a' } },
        { at : hlc.from(_constantTimestamp), peer : 2 , seq : 1, ver : 1, tab : 'testA', delta : { id : 6, tenantId : 2, name : '6a' } },
        { at : hlc.from(_constantTimestamp), peer : 2 , seq : 2, ver : 1, tab : 'testA', delta : { id : 7, tenantId : 2, name : '7a' } }, // missing A
        { at : hlc.from(_constantTimestamp), peer : 2 , seq : 4, ver : 1, tab : 'testA', delta : { id : 9, tenantId : 2, name : '9a' } },
      ];
      // Apply all patches
      for (const patch of _patches) {
        app._onPatchReceivedFromPeers(patch);
      }

      setTimeout(() => {
        // Should search and find the patch in the database  (pending_patches)
        app._onRequestForMissingPatchFromPeers({ type : 30 /* MISSING_PATCH */, peer : 3, minSeq : 3, maxSeq : 3, forPeer : 10 }); // missing B
        assert.strictEqual(messagesPerPeer[10].length, 1, 'Should have sent messages to the peer 10');
        assert.deepStrictEqual(messagesPerPeer[10][0], { type : 10 /* PATCH */, at : hlc.from(_constantTimestamp), peer : 3, seq : 3, ver : 2, tab : 'testB', delta : { id : 3, tenantId : 1, name : '3a' } });

        // Should search and find the patch in the database (testA)
        app._onRequestForMissingPatchFromPeers({ type : 30 /* MISSING_PATCH */, peer : 2, minSeq : 2, maxSeq : 2, forPeer : 2 }); // missing A
        assert.strictEqual(messagesPerPeer[2].length, 1, 'Should have sent messages to the peer 2');
        assert.deepStrictEqual(messagesPerPeer[2][0], { type : 10, at : hlc.from(_constantTimestamp), peer : 2, seq : 2, ver : 1, tab : 'testA', delta : { id : 7, tenantId : 2, name : '7a', createdAt : null, deletedAt : null } });

        // Should not crash if the patch is not found
        app._onRequestForMissingPatchFromPeers({ type : 30 /* MISSING_PATCH */, peer : 3, minSeq : 300, maxSeq : 300, forPeer : 10 });
        assert.strictEqual(messagesPerPeer[10].length, 1, 'Should not have sent new messages to the peer 10');

        // Should not crash if the peer is not found
        app._onRequestForMissingPatchFromPeers({ type : 30 /* MISSING_PATCH */, peer : 3, minSeq : 300, maxSeq : 300, forPeer : 100 });
        assert.strictEqual(messagesPerPeer[10].length, 1, 'Should not have sent new messages to the peer 10');
        done();
      }, patchApplyDelayMs);
    });

    it('should return the all missing patches (range request) to the right peer from a given sequenceId', function (done) {
      app = SQLiteOnSteroid(db, 1);
      app.addRemotePeer(10, fakePeerSockets[10]);
      app.addRemotePeer(2, fakePeerSockets[2]);
      app.migrate([{ up : _testSchema, down : ''}]);
      const _constantTimestamp = Date.now();
      const _patches = [
        { at : hlc.from(_constantTimestamp), peer : 3 , seq : 1, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 1, name : '1a' } },
        { at : hlc.from(_constantTimestamp), peer : 3 , seq : 3, ver : 1, tab : 'testA', delta : { id : 3, tenantId : 1, name : '3a' } },
        { at : hlc.from(_constantTimestamp), peer : 3 , seq : 5, ver : 1, tab : 'testA', delta : { id : 5, tenantId : 1, name : '5a' } },
        { at : hlc.from(_constantTimestamp), peer : 2 , seq : 1, ver : 1, tab : 'testA', delta : { id : 1, tenantId : 1, name : '1a' } },
        { at : hlc.from(_constantTimestamp), peer : 2 , seq : 3, ver : 2, tab : 'testB', delta : { id : 3, tenantId : 1, name : '3a' } }, // missing B
        { at : hlc.from(_constantTimestamp), peer : 2 , seq : 5, ver : 2, tab : 'testB', delta : { id : 5, tenantId : 1, name : '5a' } }, // missing B
        { at : hlc.from(_constantTimestamp), peer : 2 , seq : 6, ver : 2, tab : 'testB', delta : { id : 6, tenantId : 1, name : '6a' } }, // missing B
        { at : hlc.from(_constantTimestamp), peer : 3 , seq : 6, ver : 1, tab : 'testA', delta : { id : 6, tenantId : 2, name : '6a' } },
        { at : hlc.from(_constantTimestamp), peer : 10, seq : 2, ver : 1, tab : 'testA', delta : { id : 7, tenantId : 2, name : '7a' } },
        { at : hlc.from(_constantTimestamp), peer : 10, seq : 4, ver : 1, tab : 'testA', delta : { id : 9, tenantId : 2, name : '9a' } },
      ];
      // Apply all patches
      for (const patch of _patches) {
        app._onPatchReceivedFromPeers(patch);
      }

      setTimeout(() => {
        // Should search and find the patch in the database  (pending_patches)
        app._onRequestForMissingPatchFromPeers({ type : 30 /* MISSING_PATCH */, peer : 2, minSeq : 2, maxSeq : 5, forPeer : 10 }); // missing B
        assert.strictEqual(messagesPerPeer[10].length, 2, 'Should have sent messages to the peer 10');
        assert.deepStrictEqual(messagesPerPeer[10][0], { type : 10, at : hlc.from(_constantTimestamp), peer : 2, seq : 3, ver : 2, tab : 'testB', delta : { id : 3, tenantId : 1, name : '3a' } });
        assert.deepStrictEqual(messagesPerPeer[10][1], { type : 10, at : hlc.from(_constantTimestamp), peer : 2, seq : 5, ver : 2, tab : 'testB', delta : { id : 5, tenantId : 1, name : '5a' } });

        // Should search and find the patch in the database (testA)
        app._onRequestForMissingPatchFromPeers({ type : 30 /* MISSING_PATCH */, peer : 3, minSeq : 2, maxSeq : 100, forPeer : 2 }); // missing A
        assert.strictEqual(messagesPerPeer[2].length, 3, 'Should have sent messages to the peer 2');
        assert.deepStrictEqual(messagesPerPeer[2][0], { type : 10, at : hlc.from(_constantTimestamp), peer : 3, seq : 3, ver : 1, tab : 'testA', delta : { id : 3, tenantId : 1, name : '3a', createdAt : null, deletedAt : null } });
        assert.deepStrictEqual(messagesPerPeer[2][1], { type : 10, at : hlc.from(_constantTimestamp), peer : 3, seq : 5, ver : 1, tab : 'testA', delta : { id : 5, tenantId : 1, name : '5a', createdAt : null, deletedAt : null } });
        assert.deepStrictEqual(messagesPerPeer[2][2], { type : 10, at : hlc.from(_constantTimestamp), peer : 3, seq : 6, ver : 1, tab : 'testA', delta : { id : 6, tenantId : 2, name : '6a', createdAt : null, deletedAt : null } });

        // Should not crash if the patch is not found
        app._onRequestForMissingPatchFromPeers({ type : 30 /* MISSING_PATCH */, peer : 2, minSeq : 200, maxSeq : 200, forPeer : 10 });
        assert.strictEqual(messagesPerPeer[10].length, 2, 'Should not have sent new messages to the peer 10');

        // Should not crash if the peer is unknown
        app._onRequestForMissingPatchFromPeers({ type : 30 /* MISSING_PATCH */, peer : 2, minSeq : 2, maxSeq : 5, forPeer : 1000 });
        assert.strictEqual(messagesPerPeer[10].length, 2);
        assert.strictEqual(messagesPerPeer[2].length, 3);

        done();
      }, patchApplyDelayMs);
    });


  });

  describe('generatePeerId', function () {
    let db;
    beforeEach (function () {
      db = connect();
    });

    afterEach (function () {
      close(db);
    });

    it('should generate a unique peer id', function () {
      const _peerId = SQLiteOnSteroid(db, 1, {}).generatePeerId();
      // The peerId is constructed as: (timestamp << 13) + randomInt
      // To verify, shift right by 13 bits to get the timestamp part (in ms since 2025-01-01)
      const timestampPart = BigInt(_peerId) >> 13n;
      const msSince2025 = Date.now() - 1735689600000;
      // The timestampPart should be close to msSince2025 (allowing some delta for test execution time)
      assert.ok(Math.abs(Number(timestampPart) - msSince2025) < 1000 * 60, 'peerId timestamp part should be close to current ms since 2025-01-01');
      assert.strictEqual(_peerId < Number.MAX_SAFE_INTEGER, true, 'peerId should be a valid 53bits number below MAX_SAFE_INTEGER');
    });
  });

  describe('_parseSessionToken', function () {
    let db;
    beforeEach (function () {
      db = connect();
    });

    afterEach (function () {
      close(db);
    });

    it('should parse the session token', function () {
      const app = SQLiteOnSteroid(db, 1);
      const _sessionToken = '10.100';
      const _parsedSessionToken = app._parseSessionToken(_sessionToken);
      assert.strictEqual(_parsedSessionToken.peerId, 10);
      assert.strictEqual(_parsedSessionToken.sequenceId, 100);
      assert.strictEqual(app._parseSessionToken(' 1054545454.5454545454   ').peerId, 1054545454);
      assert.strictEqual(app._parseSessionToken(' 1054545454.5454545454   ').sequenceId, 5454545454);
      assert.strictEqual(app._parseSessionToken(' 10  . 512   ').peerId, 10);
      assert.strictEqual(app._parseSessionToken(' 10  . 512   ').sequenceId, 512);
      assert.strictEqual(app._parseSessionToken('9007199254740990.9007199254740990').sequenceId, 9007199254740990);
      assert.strictEqual(app._parseSessionToken('9007199254740990.9007199254740990').peerId, 9007199254740990);
    });

    it('should handle invalid session tokens gracefully', function () {
      const app = SQLiteOnSteroid(db, 1);
      // Test with various invalid session tokens
      const invalidTokens = [
        'invalid',
        '10.10                                                ',
        '10',
        '10.',
        '.100',
        '10.100.extra',
        'abc.def',
        '10.abc',
        'abc.100',
        '',
        null,
        undefined,
        '10.100.200.300',
        '10.100.200',
        '-100.65',
        'true.false',
        '12121545454357657565465465465465476874684768468468468468768764654646511646546874687.1'
      ];
      for (const invalidToken of invalidTokens) {
        assert.strictEqual(app._parseSessionToken(invalidToken), null);
      }
    });
  });

  describe('_backoff', function () {
    let db;
    beforeEach (function () {
      db = connect();
    });
    afterEach (function () {
      close(db);
    });
    it('should return directly if the function returns true', function () {
      const app = SQLiteOnSteroid(db, 1);
      app.backoff(() => true, (success) => {
        assert.strictEqual(success, true);
      });
      app.backoff(() => false, (success) => {
        assert.strictEqual(success, false);
      });
    });

    it('should backoff and timeout if the function does not return true', function (done) {
      const app = SQLiteOnSteroid(db, 1);
      let _nbCalls = [];
      let _previousCall = Date.now();
      const _start = Date.now();
      app.backoff(() => {
        const _now = Date.now();
        _nbCalls.push(Date.now() - _previousCall);
        _previousCall = _now;
        return false;
      }, (success) => {
        assert.strictEqual(success, false);
        // Verify the backoff timing - should start at 0 and increase exponentially with some tolerance
        const _expectedDelays = [0, 10, 20, 40, 80, 160, 320, 640, 728];
        for (let i = 0; i < _nbCalls.length; i++) {
          const _actual = _nbCalls[i];
          const _expected = _expectedDelays[i];
          assert.ok(Math.abs(_actual - _expected) <= 20, `Call ${i}: expected ~${_expected}ms, got ${_actual}ms (tolerance: 20ms)` );
        }
        assert.strictEqual(_nbCalls.length, 9, 'Should have called the function 9 times');
        assert.ok(Math.abs(Date.now() - _start - 2000) <= 20, `Should have taken ~2000ms, got ${Date.now() - _start}ms`);
        done();
      }, 2000, 10);
    });

    it('should backoff and return before timeout if the function returns true', function (done) {
      const app = SQLiteOnSteroid(db, 1);
      let _nbCalls = [];
      let _previousCall = Date.now();
      const _start = Date.now();
      app.backoff(() => {
        const _now = Date.now();
        _nbCalls.push(Date.now() - _previousCall);
        _previousCall = _now;
        if (Date.now() - _start > 30) {
          return true;
        }
        return false;
      }, (success) => {
        assert.strictEqual(success, true);
        // Verify the backoff timing - should start at 0 and increase exponentially with some tolerance
        const _expectedDelays = [0, 10, 20];
        for (let i = 0; i < _nbCalls.length; i++) {
          const _actual = _nbCalls[i];
          const _expected = _expectedDelays[i];
          assert.ok(Math.abs(_actual - _expected) <= 20, `Call ${i}: expected ~${_expected}ms, got ${_actual}ms (tolerance: 20ms)` );
        }
        assert.strictEqual(_nbCalls.length, 3, 'Should have called the function 3 times');
        done();
      }, 2000, 10);
    });

  });

});

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
