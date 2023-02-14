# TxWrap

TxWrap implements a transaction wrapper around a *sqlx.Tx transaction that simplifies error handling
and is guaranteed to either call Commit or Rollback on all transactions it starts.

TxWrap will automatically handle all database errors when using the TxWrap interface.  After an error
occurs, all future database calls using the same TxWrap will be short-circuited and fail immediately
and the transaction will be rolled back.

Usage:

```
txErr := WithTx(ctx, db, func(tx *txwrap.TxWrap) error {
    query := `SELECT sessionid FROM sessions WHERE sessionid = ?`
    if !tx.Exists(query, sessionId) {
        query = `INSERT INTO session (sessionid, counter) VALUES (?, 0)`
        tx.Exec(query, sessionId)
    }
    query = `UPDATE session SET counter = counter + 1 WHERE sessionid = ?`
    tx.Exec(query, sessionId)
    return nil
})
```

Note that no error handling is necessary.  If any of the database calls (Exists, Exec) fail
the transaction will be automatically rolled back.  If the function completes with no errors
(and the return value is nil) the transaction will be committed.  By returning a non-nil
error, you can also force a rollback at any time.
