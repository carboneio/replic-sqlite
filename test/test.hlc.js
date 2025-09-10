const assert = require('assert');
const hlc = require('../lib/hlc.js');
const MockDate = require('mockdate');

const UNIX_TIMESTAMP_OFFSET = 1735689600000n;
const MAX_TIMESTAMP = (new Date('2059-11-01')).getTime() - Number(UNIX_TIMESTAMP_OFFSET);

describe('hlc', function () {

  afterEach (function () {
    hlc._reset();
  });

  describe('toCounter', function () {
    it('should extract lowest 13 bits from HLC BigNumber', function () {
      const _hlc1 = 0; // 0 in binary
      assert.strictEqual(hlc.toCounter(_hlc1), 0);

      const _hlc2 = (1 * (2 ** 13)) - 1; // 2^13 - 1 = 8191 (all 13 bits set)
      assert.strictEqual(hlc.toCounter(_hlc2), 8191);

      const _hlc3 = (1 * (2 ** 13)) + 1; // 2^13 + 1
      assert.strictEqual(hlc.toCounter(_hlc3), 1);

      const _hlc32 = (1 * (2 ** 13)) + 2; // 2^13 + 1
      assert.strictEqual(hlc.toCounter(_hlc32), 2);

      const _hlc4 = (MAX_TIMESTAMP * (2 ** 13)) + 2045;
      assert.strictEqual(hlc.toCounter(_hlc4), 2045);

      const _hlc5 = (MAX_TIMESTAMP * (2 ** 13)) + 3;
      assert.strictEqual(hlc.toCounter(_hlc5), 3);
    });
  });

  describe('toTimestamp', function () {
    it('should extract highest bits from HLC number', function () {
      const _hlc1 = 0; // 0 in binary
      assert.strictEqual(hlc.toTimestamp(_hlc1), 0);

      const _hlc2 = 1 * (2 ** 13); // 2^13 (1 in upper bits, 0 in lower bits)
      assert.strictEqual(hlc.toTimestamp(_hlc2), 1);

      const _hlc3 = (1 * (2 ** 13)) + 3; // 2^13 + 1 (upper bits + 1 in lower bits)
      assert.strictEqual(hlc.toTimestamp(_hlc3), 1);

      const _hlc4 = (5 * (2 ** 13)) + 123; // 5 in upper bits, 123 in lower bits
      assert.strictEqual(hlc.toTimestamp(_hlc4), 5);

      // Test with larger timestamp (2059 Year in ms)
      const _hlc5 = (MAX_TIMESTAMP * (2 ** 13)) + 0;
      assert.strictEqual(hlc.toTimestamp(_hlc5), MAX_TIMESTAMP);
    });
  });

  describe('toUnixTimestamp', function () {
    it('should extract real timestamp from HLC number', function () {
      const _hlc1 = 0; // 0 in binary
      assert.strictEqual(hlc.toUnixTimestamp(_hlc1), Number(UNIX_TIMESTAMP_OFFSET));

      const _hlc2 = 1 * (2 ** 13);
      assert.strictEqual(hlc.toUnixTimestamp(_hlc2), Number(UNIX_TIMESTAMP_OFFSET) + 1);

      const _hlc3 = (1500 * (2 ** 13)) + 3;
      assert.strictEqual(hlc.toUnixTimestamp(_hlc3), Number(UNIX_TIMESTAMP_OFFSET) + 1500);

      const _hlc4 = (5 * (2 ** 13)) + 123;
      assert.strictEqual(hlc.toUnixTimestamp(_hlc4), Number(UNIX_TIMESTAMP_OFFSET) + 5);

      const _hlc5 = (MAX_TIMESTAMP * (2 ** 13)) + 0;
      assert.strictEqual(hlc.toUnixTimestamp(_hlc5), (new Date('2059-11-01')).getTime());
    });
  });

  describe('from', function () {
    it('should convert a timestamp and a counter to a HLC BigNumber', function () {
      assert.strictEqual(hlc.from(1735689600000, 0), Number(((1735689600000n - UNIX_TIMESTAMP_OFFSET) << 11n) + 0n));
      assert.strictEqual(hlc.from(1735689600000, 1), Number(((1735689600000n - UNIX_TIMESTAMP_OFFSET) << 11n) + 1n));
      assert.strictEqual(hlc.from(1735689600000, 2), Number(((1735689600000n - UNIX_TIMESTAMP_OFFSET) << 11n) + 2n));
    });
  });

  describe('receive and create hlc', function () {
    it('should receive a remote HLC', function () {
      const hr = process.hrtime;
      const start = hr();
      for (let i = 0; i < 1000; i++) {
        hlc.create();
      }
      const diff = hr(start);
      const ms = diff[0] * 1000 + diff[1] / 1e6;
      console.log(`hlc.create() x 100000 took ${ms.toFixed(2)} ms`);
    });
  });

  describe('receive and create hlc', function () {
    it('should receive a remote HLC', function () {
      MockDate.set(new Date('2025-01-01')); // 1735689600000
      hlc.receive(hlc.from(1735689600011, 0)); // receive a remote HLC with timestamp in the future
      assert.strictEqual(hlc.create(), hlc.from(1735689600011, 1));
      assert.strictEqual(hlc.create(), hlc.from(1735689600011, 2));
      assert.strictEqual(hlc.create(), hlc.from(1735689600011, 3));

      hlc.receive(hlc.from(1735689600011, 2)); // receive a remote HLC with same timestamp but higher counter
      assert.strictEqual(hlc.create(), hlc.from(1735689600011, 6));
      assert.strictEqual(hlc.create(), hlc.from(1735689600011, 7));

      hlc.receive(hlc.from(1735689600011, 0)); // receive a remote HLC with same timestamp but lower counter
      assert.strictEqual(hlc.create(), hlc.from(1735689600011, 8));

      hlc.receive(hlc.from(1735689500000, 0)); // receive a remote HLC with lower timestamp
      assert.strictEqual(hlc.create(), hlc.from(1735689600011, 9));

      hlc.receive(hlc.from(1735689600012, 0)); // receive a remote HLC with new timestamp (reset the local counter)
      assert.strictEqual(hlc.create(), hlc.from(1735689600012, 1));

      MockDate.set(new Date('2025-01-02')); // 1735776000000
      // If timestamp is higher than last remote, it can return the same timestamp because it relies on sequence id order
      assert.strictEqual(hlc.create(), hlc.from(1735776000000, 0));
      assert.strictEqual(hlc.create(), hlc.from(1735776000000, 0));
      MockDate.reset();
    });
  });



});

