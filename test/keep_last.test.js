const assert = require('assert');
const Database = require('better-sqlite3');
const path = require('path');

describe('keepLast extension', () => {
  let db;

  before(() => {
    db = new Database(':memory:');
    db.loadExtension(path.resolve(__dirname, '../build/Release/keep_last'));

    // Create and populate test table
    db.exec(`
      CREATE TABLE test_data (
        id INTEGER PRIMARY KEY,
        value TEXT
      );
      
      INSERT INTO test_data (id, value) VALUES
        (1, 'A'),
        (2, NULL),
        (3, NULL),
        (4, 'B'),
        (5, NULL),
        (6, 'C'),
        (7, NULL),
        (8, NULL)
    `);
  });

  after(() => {
    db.close();
  });

  it.skip('should keep last non-null value in window', () => {
    //
    const rows = db.prepare(`
      SELECT 
        id,
        value,
        keep_last(value, id) OVER (ORDER BY id) as kept_value
      FROM test_data
      ORDER BY id
    `).all();

    assert.deepStrictEqual(rows, [
      { id : 1, value : 'A', kept_value : 'A' },
      { id : 2, value : null, kept_value : 'A' },
      { id : 3, value : null, kept_value : 'A' },
      { id : 4, value : 'B', kept_value : 'B' },
      { id : 5, value : null, kept_value : 'B' },
      { id : 6, value : 'C', kept_value : 'C' },
      { id : 7, value : null, kept_value : 'C' },
      { id : 8, value : null, kept_value : 'C' }
    ]);
  });

  it('should keep last value ', () => {
    db.exec(`
      CREATE TABLE categorized_data (
        id INTEGER PRIMARY KEY,
        category TEXT,
        value TEXT
      );

      INSERT INTO categorized_data (id, category, value) VALUES
        (1, 'A', 'A1'),
        (2, 'A', NULL),
        (3, 'A', 'A2'),
        (4, 'B', 'B1'),
        (5, 'B', NULL),
        (6, 'B', NULL),
        (7, 'A', NULL),
        (8, 'B', 'B2');
    `);

    const rows = db.prepare(`
      SELECT 
        max(id) as id,
        category,
        value,
        keep_last(value, id, 0, 0) AS kept_value
      FROM categorized_data
      GROUP BY category
    `).all();

    assert.deepStrictEqual(rows, [
      { id : 7, category : 'A', value : null, kept_value : 'A2' },
      { id : 8, category : 'B', value : 'B2', kept_value : 'B2' }
    ]);
  });

  it.skip('should handle all NULL values', () => {
    const rows = db.prepare(`
      WITH all_nulls(id, value) AS (
        VALUES
          (1, NULL),
          (2, NULL),
          (3, NULL)
      )
      SELECT 
        id,
        value,
        keepLast(value) OVER (ORDER BY id) as kept_value
      FROM all_nulls
      ORDER BY id
    `).all();

    assert.deepStrictEqual(rows, [
      { id : 1, value : null, kept_value : null },
      { id : 2, value : null, kept_value : null },
      { id : 3, value : null, kept_value : null }
    ]);
  });

  it.skip('should handle different data types', () => {
    const rows = db.prepare(`
      WITH mixed_types(id, value) AS (
        VALUES
          (1, 42),
          (2, NULL),
          (3, 3.14),
          (4, NULL),
          (5, 'text'),
          (6, NULL)
      )
      SELECT 
        id,
        value,
        keepLast(value) OVER (ORDER BY id) as kept_value
      FROM mixed_types
      ORDER BY id
    `).all();

    assert.deepStrictEqual(rows, [
      { id : 1, value : 42, kept_value : 42 },
      { id : 2, value : null, kept_value : 42 },
      { id : 3, value : 3.14, kept_value : 3.14 },
      { id : 4, value : null, kept_value : 3.14 },
      { id : 5, value : 'text', kept_value : 'text' },
      { id : 6, value : null, kept_value : 'text' }
    ]);
  });
});