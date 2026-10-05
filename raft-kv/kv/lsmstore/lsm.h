/* Copied from lsm-engine/capi/include/lsm.h; keep in sync. */
/* C ABI for lsm-engine. All functions are thread-safe on one lsm_db.
 *
 * Errors: functions taking `char **err` set *err to a NUL-terminated
 * message on failure (free with lsm_free_err) and leave it NULL on success.
 * Buffers returned through `uint8_t **` are owned by the caller and must
 * be released with lsm_free(ptr, len). Iterator key/value pointers are
 * borrowed and valid only until the next lsm_iter_next/lsm_iter_free.
 * Rust panics never cross this boundary; they are reported as errors.
 */
#ifndef LSM_H
#define LSM_H
#include <stddef.h>
#include <stdint.h>

typedef struct lsm_db lsm_db;
typedef struct lsm_batch lsm_batch;
typedef struct lsm_iter lsm_iter;

/* sync != 0: every write fsyncs the WAL by default. */
lsm_db *lsm_open(const char *path, int sync, char **err);
void lsm_close(lsm_db *db);

/* Returns 1 if found (value in *val/*vlen), 0 if absent, -1 on error. */
int lsm_get(lsm_db *db, const uint8_t *key, size_t klen, uint8_t **val, size_t *vlen, char **err);

lsm_batch *lsm_batch_new(void);
void lsm_batch_put(lsm_batch *b, const uint8_t *key, size_t klen, const uint8_t *val, size_t vlen);
void lsm_batch_delete(lsm_batch *b, const uint8_t *key, size_t klen);
void lsm_batch_free(lsm_batch *b);
/* Applies the batch atomically; the batch is not consumed. 0 ok, -1 error. */
int lsm_write(lsm_db *db, const lsm_batch *b, int sync, char **err);

/* Consistent iterator over [start, end); a NULL bound is open. */
lsm_iter *lsm_scan(lsm_db *db, const uint8_t *start, size_t slen, const uint8_t *end, size_t elen);
/* 1 = an entry was produced, 0 = end (check lsm_iter_status). */
int lsm_iter_next(lsm_iter *it, const uint8_t **key, size_t *klen, const uint8_t **val, size_t *vlen);
int lsm_iter_status(const lsm_iter *it, char **err);
void lsm_iter_free(lsm_iter *it);

/* Writes the memtable to an SST and waits (makes all prior writes durable
 * even with sync = 0). 0 ok, -1 error. */
int lsm_flush(lsm_db *db, char **err);

void lsm_free(uint8_t *ptr, size_t len);
void lsm_free_err(char *err);

#endif
