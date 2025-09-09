

function removeLastTimestampInStats (stat) {
  const _newStat = {};
  for (const key in stat) {
    if (stat[key].length === 5) {
      _newStat[key] = stat[key].slice(0, -1);
    }
  }
  return _newStat;
}

function simplifyStats (patches) {
  for (const patch of patches) {
    if (patch.type === 20 /* PING */ && patch.delta) {
      patch.delta = removeLastTimestampInStats(patch.delta);
    }
  }
  return patches;
}


module.exports = {
  removeLastTimestampInStats,
  simplifyStats
};