#include <sqlite3ext.h>
#include <stddef.h>  // <-- Add this line for NULL
SQLITE_EXTENSION_INIT1

typedef struct {
    char isInit;      /* True upon initialization */
    sqlite3_value *last_value;
    sqlite3_int64 patched_at;
    sqlite3_int64 peer_id;
    sqlite3_int64 sequence_id;
} KeepLastCtx;

static void keep_last_step(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    // NULL means "column not set by this patch": it must never win, otherwise the result depends on the order of rows
    if (argc < 4 || sqlite3_value_type(argv[0]) == SQLITE_NULL) return;

    KeepLastCtx *p = (KeepLastCtx *)sqlite3_aggregate_context(ctx, sizeof(*p));
    if( p==0 ) return;  // Handle allocation failure

    sqlite3_int64 new_patched_at = sqlite3_value_int64(argv[1]);
    sqlite3_int64 new_peer_id = sqlite3_value_int64(argv[2]);
    sqlite3_int64 new_sequence_id = sqlite3_value_int64(argv[3]);
    // Keep the first value, or replace it if the new priority is higher
    if (!p->isInit ||
        new_patched_at > p->patched_at ||
        (new_patched_at == p->patched_at &&
            (new_peer_id > p->peer_id ||
            (new_peer_id == p->peer_id && new_sequence_id > p->sequence_id)))) {
        if (p->last_value) {
            sqlite3_value_free(p->last_value);
        }
        p->isInit = 1;
        p->last_value = sqlite3_value_dup(argv[0]);
        p->patched_at = new_patched_at;
        p->peer_id = new_peer_id;
        p->sequence_id = new_sequence_id;
    }
}

static void keep_last_finalize(sqlite3_context *ctx) {
    KeepLastCtx *p = (KeepLastCtx *)sqlite3_aggregate_context(ctx, 0);
    if (p && p->last_value) {
        sqlite3_result_value(ctx, p->last_value);
        sqlite3_value_free(p->last_value);
        p->last_value = NULL;
    } else {
        sqlite3_result_null(ctx);
    }
}

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_keeplast_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    SQLITE_EXTENSION_INIT2(pApi);

    // Register as an aggregate function
    return sqlite3_create_function(
        db,
        "keep_last",
        4,
        SQLITE_UTF8|SQLITE_INNOCUOUS|SQLITE_DETERMINISTIC,
        0,
        0,
        keep_last_step,
        keep_last_finalize
    );
}
