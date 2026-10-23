/**
 * Hybrid Logical Clocks
 *
 * Generate and extract information from the HLC Number stored in patchedAt column
 * This number is composed of 53 bits maximum (less than MAX_SAFE_NUMBER)
 * 13 bits for the counter   : 8192 values per millisecond. Beyond that, the counter carries over to the timestamp bits:
 *                             the order is preserved, the HLC is just a little ahead of the real time until the clock catches up
 * 40 bits for the timestamp : unix timestamp in ms starting from 2025-01-01 (1735689600000), valid until 2059
 *
 * Hybrid Logical Clocks (HLC) are used to generate a timestamp that is globally consistent.
 * Make the system robust to clock drift.
 *
 */

let highestRemoteHLC = 0;
let lastLocalHLC = 0;
let clockDriftHLC = 0;
let isClockDriftReported = false;
const UNIX_TIMESTAMP_OFFSET = 1735689600000;
const CLOCK_DRIFT_WARNING_MS = 1000;
const CLOCK_DRIFT_WARNING_HLC = CLOCK_DRIFT_WARNING_MS * (2 ** 13);

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
    highestRemoteHLC = remoteHLC;
  }
}

/**
 * Create a new HLC, never lower than the previous one
 *
 * @returns {number}
 */
function create () {
  const _now = (Date.now() - UNIX_TIMESTAMP_OFFSET) * (2 ** 13);
  if (_now > highestRemoteHLC && _now >= lastLocalHLC) {
    // Locally, a node can return the same timestamp upon multiple request of create()
    // because the patches are sorted by sequence id
    lastLocalHLC = _now;
    isClockDriftReported = false;
    return _now;
  }
  // The local clock is behind a remote peer, or went backwards (NTP adjustment, restart): increment the counter
  lastLocalHLC = Math.max(lastLocalHLC, highestRemoteHLC) + 1;
  clockDriftHLC = lastLocalHLC - _now;
  // Report only once until the local clock catches up, to avoid flooding logs
  if (isClockDriftReported === false && clockDriftHLC > CLOCK_DRIFT_WARNING_HLC) {
    isClockDriftReported = true;
    console.warn(`WARN: clock drift of ${getClockDriftMs()} ms. Make sure system time is synchronized with NTP on all peers`);
  }
  return lastLocalHLC;
}

/**
 * Only for testing purposes
 */
function _reset () {
  highestRemoteHLC = 0;
  lastLocalHLC = 0;
  isClockDriftReported = false;
}

function getClockDriftMs () {
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