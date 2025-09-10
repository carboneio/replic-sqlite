

CREATE TABLE IF NOT EXISTS migrations (
  id     INTEGER NOT NULL,
  up     TEXT    NOT NULL,
  down   TEXT    NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS pending_patches (
  _patchedAt    INTEGER  NOT NULL, /* 53bits number (HLC timestamp  + counter) -*/
  _peerId       INTEGER  NOT NULL, /* 53bits globally unique, Source of change */
  _sequenceId   INTEGER  NOT NULL, /* 53bits number, Consecutive sequence number of change per peer */
  patchVersion  INTEGER  NOT NULL, /* db version */
  tableName     TEXT     NOT NULL, /* Can be empty for ping message */
  delta         BLOB     NOT NULL  /* json patch */
) STRICT;

CREATE INDEX IF NOT EXISTS pending_patches_at_idx ON pending_patches (_patchedAt);