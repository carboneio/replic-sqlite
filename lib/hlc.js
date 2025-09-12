/**
 * Hybrid Logical Clocks
 *
 * Generate and extract information from the HLC Number stored in patchedAt column
 * This number is composed of 53 bits maximum (less than MAX_SAFE_NUMBER)
 * 13 bits for the counter   : valid for 2^13 - 1 = 8191 patches at the same timestamp in milliseconds
 * 40 bits for the timestamp : unix timestamp in ms starting from 2025-01-01 (1735689600000)
 *
 * Hybrid Logical Clocks (HLC) are used to generate a timestamp that is globally consistent.
 * Make the system robust to clock drift.
 *
 */

let highestRemoteHLC = 0;
let counter = 0;
let clockDriftHLC = 0;
const UNIX_TIMESTAMP_OFFSET = 1735689600000;

/**
 * Extract the lowest 11 bits of the BigNumber HLC parameter
 *
 * @param {BigNumber} hlc Hybrid Logical Clocks
 * @returns {number}
 */
function toCounter (hlc) {
  return Number(BigInt(hlc) & ((1n << 13n) - 1n));
}

/**
 * Extract the highest bits of the BigNumber HLC parameter
 *
 * @param {BigNumber} hlc Hybrid Logical Clocks
 * @returns {number}
 */
function toUnixTimestamp (hlc) {
  return toTimestamp(hlc) + UNIX_TIMESTAMP_OFFSET;
}

/**
 * Extract the highest bits of the BigNumber HLC parameter
 *
 * @param {BigNumber} hlc Hybrid Logical Clocks
 * @returns {number}
 */
function toTimestamp (hlc) {
  return Number(BigInt(hlc) >> 13n);
}


/**
 * Convert a timestamp and a counter to a HLC BigNumber
 *
 * @param {number} unixTimestamp
 * @param {number} counter
 * @returns {number}
 */
function from (unixTimestamp, counter = 0) {
  // multiply by 2^13 instead of shifting by 13 bits because it does not work on Numbers (sign bit issue)
  // And it is faster than BigInt conversion
  return (unixTimestamp - UNIX_TIMESTAMP_OFFSET) * (2 ** 13) + counter;
}

/**
 * Receive a remote HLC
 *
 * @param {number} remoteHLC
 */
function receive (remoteHLC) {
  if (remoteHLC > highestRemoteHLC) {
    if (toTimestamp(remoteHLC) > toTimestamp(highestRemoteHLC)) {
      counter = 0;
    }
    highestRemoteHLC = remoteHLC;
  }
}

/**
 * Create a new HLC
 *
 * @returns {number}
 */
function create () {
  const _now = (Date.now() - UNIX_TIMESTAMP_OFFSET) * (2 ** 13);
  if (_now > highestRemoteHLC) {
    counter = 0;
    // If _now is greater than remote timestamp, return _now.
    // Locally, a node can return the same timestamp upon multiple request of create()
    // because the patches are sorted by sequence id
    return _now;
  }
  counter++;
  if (counter > 8191) {
    console.warn('WARN: clock skew too high. Make sure system time is synchronized with NTP');
  }
  clockDriftHLC = highestRemoteHLC - _now;
  return highestRemoteHLC + counter;
}

/**
 * Only for testing purposes
 */
function _reset() {
  highestRemoteHLC = 0;
  counter = 0;
}

function getClockDriftMs() {
  return toTimestamp(clockDriftHLC);
}

module.exports = {
  UNIX_TIMESTAMP_OFFSET,
  toCounter,
  toTimestamp,
  toUnixTimestamp,
  from,
  receive,
  create,
  _reset,
  getClockDriftMs
};