const assert = require('assert');
const SQLiteOnSteroid = require('../lib/index.js');
const path = require('path');
const fs = require('fs');
const Database  = require('better-sqlite3');
const debug = require('debug');

describe('migrate', function () {
  let db, app;

  beforeEach(function () {
    db = connect(); // memory db
    app = SQLiteOnSteroid(db);

  });

  afterEach(function () {
    close(db);
  });

  describe('upgrade', function () {
    it('should apply new migrations in order', function () {
      const appMigrations = [
        {
          up   : 'CREATE TABLE test1 (id INTEGER PRIMARY KEY)',
          down : 'DROP TABLE test1'
        },
        {
          up   : 'CREATE TABLE test2 (id INTEGER PRIMARY KEY)',
          down : 'DROP TABLE test2'
        }
      ];

      const result = app.migrate(appMigrations);

      // Verify result
      assert.deepStrictEqual(result, {
        currentVersion  : 2,
        previousVersion : 0
      });

      // Verify tables were created
      const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all();
      assert.ok(tables.some(t => t.name === 'test1'));
      assert.ok(tables.some(t => t.name === 'test2'));

      // Verify migrations were recorded
      const migrations = db.prepare('SELECT * FROM migrations ORDER BY id').all();
      assert.strictEqual(migrations.length, 2);
      assert.strictEqual(migrations[0].id, 1);
      assert.strictEqual(migrations[0].up, 'CREATE TABLE test1 (id INTEGER PRIMARY KEY)');
      assert.strictEqual(migrations[1].id, 2);
      assert.strictEqual(migrations[1].up, 'CREATE TABLE test2 (id INTEGER PRIMARY KEY)');
    });

    it('should do nothing if already at target version', function () {
      // First apply some migrations
      const initialMigrations = [
        {
          up   : 'CREATE TABLE test1 (id INTEGER PRIMARY KEY)',
          down : 'DROP TABLE test1'
        }
      ];
      app.migrate(initialMigrations);

      // Try to apply the same migrations again
      const result = app.migrate(initialMigrations);

      assert.deepStrictEqual(result, {
        currentVersion  : 1,
        previousVersion : 1
      });

      // Verify no duplicate tables were created
      const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all();
      assert.strictEqual(tables.filter(t => t.name === 'test1').length, 1);
    });

    it('should execute dependent migrations in correct order during upgrade', function () {
      // Create migrations that depend on each other
      const dependentMigrations = [
        {
          up   : 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)',
          down : 'DROP TABLE users'
        },
        {
          up   : 'ALTER TABLE users RENAME COLUMN name TO full_name',
          down : 'ALTER TABLE users RENAME COLUMN full_name TO name'
        },
        {
          up   : 'ALTER TABLE users RENAME COLUMN full_name TO display_name',
          down : 'ALTER TABLE users RENAME COLUMN display_name TO full_name'
        }
      ];

      // Execute migrations
      const result = app.migrate(dependentMigrations);

      assert.deepStrictEqual(result, {
        currentVersion  : 3,
        previousVersion : 0
      });

      // Verify table was created and columns were renamed in correct order
      const tableInfo = db.prepare('PRAGMA table_info(users)').all();
      assert.strictEqual(tableInfo.length, 2); // id and display_name columns
      assert.ok(tableInfo.some(col => col.name === 'id'));
      assert.ok(tableInfo.some(col => col.name === 'display_name'));
      assert.ok(!tableInfo.some(col => col.name === 'name'));
      assert.ok(!tableInfo.some(col => col.name === 'full_name'));
    });
  });

  describe('downgrade', function () {
    it('should rollback migrations in reverse order', function () {
      // First apply some migrations
      const initialMigrations = [
        {
          up   : 'CREATE TABLE test1 (id INTEGER PRIMARY KEY)',
          down : 'DROP TABLE test1'
        },
        {
          up   : 'CREATE TABLE test2 (id INTEGER PRIMARY KEY)',
          down : 'DROP TABLE test2'
        }
      ];
      app.migrate(initialMigrations);

      // Now downgrade to version 1
      const downgradeMigrations = [initialMigrations[0]];
      const result = app.migrate(downgradeMigrations);

      assert.deepStrictEqual(result, {
        currentVersion  : 1,
        previousVersion : 2
      });

      // Verify test2 table was dropped
      const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all();
      assert.ok(tables.some(t => t.name === 'test1'));
      assert.ok(!tables.some(t => t.name === 'test2'));

      // Verify migrations table was updated
      const migrations = db.prepare('SELECT * FROM migrations ORDER BY id').all();
      assert.strictEqual(migrations.length, 1);
      assert.strictEqual(migrations[0].id, 1);
    });

    it('should handle complete rollback', function () {
      // First apply some migrations
      const initialMigrations = [
        {
          up   : 'CREATE TABLE test1 (id INTEGER PRIMARY KEY)',
          down : 'DROP TABLE test1'
        }
      ];
      app.migrate(initialMigrations);

      // Now rollback completely
      const result = app.migrate([]);

      assert.deepStrictEqual(result, {
        currentVersion  : 0,
        previousVersion : 1
      });

      // Verify table was dropped
      const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all();
      assert.ok(!tables.some(t => t.name === 'test1'));

      // Verify migrations table is empty
      const migrations = db.prepare('SELECT * FROM migrations').all();
      assert.strictEqual(migrations.length, 0);
    });

    it('should execute dependent migrations in correct order during downgrade', function () {
      // First apply all migrations
      const dependentMigrations = [
        {
          up   : 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)',
          down : 'DROP TABLE users'
        },
        {
          up   : 'ALTER TABLE users RENAME COLUMN name TO full_name',
          down : 'ALTER TABLE users RENAME COLUMN full_name TO name'
        },
        {
          up   : 'ALTER TABLE users RENAME COLUMN full_name TO display_name',
          down : 'ALTER TABLE users RENAME COLUMN display_name TO full_name'
        }
      ];
      app.migrate(dependentMigrations);

      // Now downgrade to version 1 (original table structure)
      const downgradeMigrations = [dependentMigrations[0]];
      const result = app.migrate(downgradeMigrations);

      assert.deepStrictEqual(result, {
        currentVersion  : 1,
        previousVersion : 3
      });

      // Verify table structure was restored to original state
      const tableInfo = db.prepare('PRAGMA table_info(users)').all();
      assert.strictEqual(tableInfo.length, 2); // id and name columns
      assert.ok(tableInfo.some(col => col.name === 'id'));
      assert.ok(tableInfo.some(col => col.name === 'name'));
      assert.ok(!tableInfo.some(col => col.name === 'full_name'));
      assert.ok(!tableInfo.some(col => col.name === 'display_name'));

      // Verify migrations table was updated correctly
      const migrations = db.prepare('SELECT * FROM migrations ORDER BY id').all();
      assert.strictEqual(migrations.length, 1);
      assert.strictEqual(migrations[0].id, 1);
    });
  });

  describe('error handling', function () {
    it('should rollback transaction if migration fails', function () {
      const appMigrations = [
        {
          up   : 'CREATE TABLE test1 (id INTEGER PRIMARY KEY)',
          down : 'DROP TABLE test1'
        },
        {
          up   : 'INVALID SQL STATEMENT', // This will fail
          down : 'DROP TABLE test2'
        }
      ];

      assert.throws(() => app.migrate(appMigrations));

      // Verify no tables were created
      const tables = db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all();
      assert.ok(!tables.some(t => t.name === 'test1'));

      // Verify no migrations were recorded
      const migrations = db.prepare('SELECT * FROM migrations').all();
      assert.strictEqual(migrations.length, 0);
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
  // Return numbers (TODO convert to string constantId)
  db.defaultSafeIntegers(false);

  return db;
}

function close (db) {
  db?.close?.();
}
