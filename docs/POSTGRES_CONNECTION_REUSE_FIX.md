# PostgreSQL Connection Reuse Fix

## Problem: "Idle in Transaction" Connection Leaks

When switching from Dagster's default `NullPool` to `QueuePool` for connection reuse, a critical issue emerges: **connections can get stuck in "idle in transaction" state**, preventing them from being reused.

### Root Cause

**Dagster was designed for NullPool:**
- NullPool creates a new connection for each operation
- Connection is **destroyed** after `.close()` is called
- No connection state persists between operations
- Connection leaks are impossible

**QueuePool behaves differently:**
- Connections are **reused** from a pool
- `.close()` **returns connection to pool** (doesn't destroy it)
- Connection state **persists** (transactions, session variables, temp tables)
- **Connection leaks ARE possible** if state isn't cleaned up

### The Specific Problem

Dagster uses `isolation_level="AUTOCOMMIT"` on all connections, which means:
- No automatic transaction wrapping
- Each SQL statement auto-commits
- **UNLESS** code explicitly starts a transaction with `conn.begin()`

However, Dagster's upstream code contains patterns like this:

```python
# From SqlRunStorage.add_run_tags()
def add_run_tags(self, run_id: str, new_tags: Mapping[str, str]) -> None:
    with self.connect() as conn:
        # Multiple UPDATE statements
        conn.execute(RunsTable.update()...)
        conn.execute(RunTagsTable.update()...)
        conn.execute(RunTagsTable.insert()...)
```

When an error occurs mid-operation:
1. Connection starts executing statements
2. Error is raised before all statements complete
3. `conn.close()` is called in finally block
4. Connection returned to pool **still in active transaction state**
5. Next checkout gets a connection that's "idle in transaction"
6. PostgreSQL won't let that connection be reused until transaction ends
7. Connection is effectively **leaked** from the pool

### Why This Causes Pool Exhaustion

```
QueuePool limit of size 10 overflow 20 reached, connection timed out, timeout 30.00
```

This error means:
- All 30 connections (10 pool + 20 overflow) are checked out
- Operations are waiting 30 seconds for a connection to become available
- Connections aren't being returned to pool in **usable state**

## Solution: pool_reset_on_return='rollback'

SQLAlchemy provides `pool_reset_on_return` parameter that automatically resets connection state when returned to pool:

```python
self._engine = create_engine(
    self.postgres_url,
    isolation_level="AUTOCOMMIT",
    poolclass=db_pool.QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_recycle=3600,
    pool_timeout=30,
    pool_pre_ping=True,
    pool_reset_on_return="rollback",  # <-- THE FIX
)
```

### How pool_reset_on_return='rollback' Works

When a connection is returned to the pool via `conn.close()`:

1. SQLAlchemy checks if connection has an uncommitted transaction
2. If yes, automatically issues `ROLLBACK`
3. Connection is returned to pool in clean state
4. Next checkout gets a connection with no active transaction

This is **exactly what we need** because:
- Handles Dagster's incomplete transaction patterns
- No code changes needed in upstream Dagster
- Transparent to application code
- Minimal performance overhead (only rollback if needed)

### Alternative Values

`pool_reset_on_return` can be:
- `None`: No reset (dangerous with QueuePool + AUTOCOMMIT)
- `"rollback"`: Issue ROLLBACK if transaction active (our choice)
- `"commit"`: Issue COMMIT if transaction active (risky - might commit partial data)
- Callable: Custom reset function

We chose `"rollback"` because:
- Safe: Never commits incomplete transactions
- Exactly matches what finally block would do on exception
- Handles both explicit (`conn.begin()`) and implicit transactions

## Implementation

Applied to all three storage classes:
- `PooledPostgresRunStorage`
- `PooledPostgresEventLogStorage`
- `PooledPostgresScheduleStorage`

Both in `__init__` and `optimize_for_webserver()` methods.

## Verification

To verify connections are being properly reset:

```sql
-- Check for idle in transaction connections
SELECT
    pid,
    state,
    state_change,
    wait_event_type,
    query,
    now() - state_change AS duration
FROM pg_stat_activity
WHERE
    datname = 'dagster'
    AND state = 'idle in transaction'
ORDER BY state_change;
```

After this fix, you should see:
- Zero or very few "idle in transaction" connections
- Connections in "idle" state (good - waiting for reuse)
- No pool exhaustion errors

## References

- [SQLAlchemy pool_reset_on_return docs](https://docs.sqlalchemy.org/en/20/core/pooling.html#reset-on-return)
- [PostgreSQL transaction states](https://www.postgresql.org/docs/current/monitoring-stats.html#MONITORING-PG-STAT-ACTIVITY-VIEW)
- [Dagster issue #8123](https://github.com/dagster-io/dagster/issues/8123) - Connection exhaustion with multi-asset materializations
- Our investigation: `upstream_patterns.md`

## Related Improvements

This fix works in conjunction with other pooling improvements:

1. **pool_timeout=30**: Max wait time for connection from pool
2. **pool_pre_ping=True**: Test connection health before checkout
3. **pool_recycle=3600**: Recycle connections every hour to prevent stale connections
4. **Configurable pool sizes**: Tune for workload via YAML config

Together, these create a robust connection pooling strategy that handles Dagster's connection patterns safely.
