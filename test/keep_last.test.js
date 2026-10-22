const assert = require('assert');
const Database = require('better-sqlite3');
const path = require('path');

describe('keepLast extension', () => {
  let db;

  before(() => {
    db = new Database(':memory:');
    db.loadExtension(path.resolve(__dirname, '../build/Release/keep_last'));
  });

  after(() => {
    db.close();
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

  it('should ignore NULL values even if the first row read has the highest priority', () => {
    const _query = db.prepare(`
      SELECT keep_last(value, patchedAt, peerId, sequenceId) AS kept_value FROM (
        SELECT NULL AS value, 20 AS patchedAt, 1 AS peerId, 2 AS sequenceId
        UNION ALL
        SELECT 'a'  AS value, 10 AS patchedAt, 1 AS peerId, 1 AS sequenceId
      )
    `);
    assert.deepStrictEqual(_query.get(), { kept_value : 'a' });
  });

  it('should return the same value whatever the order of rows', () => {
    const _rows = [
      'SELECT \'a\'  AS value, 10 AS patchedAt, 1 AS peerId, 1 AS sequenceId',
      'SELECT NULL AS value, 10 AS patchedAt, 1 AS peerId, 2 AS sequenceId', // same timestamp, higher sequenceId
      'SELECT \'b\'  AS value, 10 AS patchedAt, 0 AS peerId, 9 AS sequenceId', // same timestamp, lower peerId
      'SELECT NULL AS value, 30 AS patchedAt, 2 AS peerId, 1 AS sequenceId'  // most recent
    ];
    const _orders = [[0, 1, 2, 3], [3, 2, 1, 0], [1, 3, 0, 2], [2, 0, 3, 1]];
    for (const _order of _orders) {
      const _union = _order.map(i => _rows[i]).join(' UNION ALL ');
      const _result = db.prepare(`SELECT keep_last(value, patchedAt, peerId, sequenceId) AS kept_value FROM (${_union})`).get();
      assert.deepStrictEqual(_result, { kept_value : 'a' }, `order ${_order}`);
    }
  });

  it('should return NULL if all values are NULL', () => {
    const _result = db.prepare(`
      SELECT keep_last(value, patchedAt, peerId, sequenceId) AS kept_value FROM (
        SELECT NULL AS value, 1 AS patchedAt, 1 AS peerId, 1 AS sequenceId
        UNION ALL
        SELECT NULL AS value, 2 AS patchedAt, 1 AS peerId, 2 AS sequenceId
      )
    `).get();
    assert.deepStrictEqual(_result, { kept_value : null });
  });

  it('should keep the type of the value', () => {
    for (const _value of [42, 3.14, 'text', Buffer.from('ab')]) {
      const _result = db.prepare(`
        SELECT keep_last(value, patchedAt, peerId, sequenceId) AS kept_value FROM (
          SELECT ? AS value, 1 AS patchedAt, 1 AS peerId, 1 AS sequenceId
          UNION ALL
          SELECT NULL AS value, 2 AS patchedAt, 1 AS peerId, 2 AS sequenceId
        )
      `).get(_value);
      assert.deepStrictEqual(_result, { kept_value : _value });
    }
  });
});
