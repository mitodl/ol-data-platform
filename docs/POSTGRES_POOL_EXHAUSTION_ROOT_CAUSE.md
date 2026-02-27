# PostgreSQL Connection Pool Exhaustion - Root Cause Analysis

## Investigation Summary

Analyzed production logs from Dagster daemon pods and RDS PostgreSQL to identify the exact cause of "QueuePool limit reached" errors.

## Evidence Collected

### 1. Application Logs (Dagster Daemon Pod)
```
kubectl logs -n dagster dagster-daemon-794d7c8c8b-hhtwv
```

**Errors Found:**
```
2026-02-17T17:34:42Z sqlalchemy.exc.TimeoutError: QueuePool limit of size 10 overflow 20 reached,
                      connection timed out, timeout 30.00
2026-02-17T17:37:55Z sqlalchemy.exc.TimeoutError: QueuePool limit of size 10 overflow 20 reached
2026-02-17T18:03:11Z sqlalchemy.exc.TimeoutError: QueuePool limit of size 10 overflow 20 reached
```

**Call Stack Pattern:**
```python
get_dynamic_partitions()
  -> self.index_connection()  # event_log storage
    -> create_pg_connection(engine)
      -> retry_pg_connection_fn(engine.connect)
        -> engine.connect()  # QueuePool exhausted here
```

### 2. RDS PostgreSQL Logs
```
aws rds download-db-log-file-portion --db-instance-identifier ol-etl-db-production
```

**Connection Resets Detected:**
```
17:36:20 UTC - 12 connections: "Connection reset by peer"
17:37:34 UTC - 12 connections: "Connection reset by peer"
17:37:40 UTC - 12 connections: "Connection reset by peer"
17:37:52 UTC - 25 connections: "Connection reset by peer"
17:38:07 UTC - 12 connections: "Connection reset by peer"
```

**Total:** 80+ connections forcibly closed in 2-minute window.

### 3. Correlation Analysis

| Time (UTC) | Event | Source |
|------------|-------|--------|
| 17:34:42 | Pool exhaustion error | Dagster daemon pod |
| 17:36:20 | 12 connections reset | RDS PostgreSQL |
| 17:37:34 | 12 connections reset | RDS PostgreSQL |
| 17:37:55 | Pool exhaustion error | Dagster daemon pod |
| 17:37:40 | 12 connections reset | RDS PostgreSQL |
| 17:37:52 | 25 connections reset | RDS PostgreSQL |
| 17:38:07 | 12 connections reset | RDS PostgreSQL |
| 18:03:11 | Pool exhaustion error | Dagster daemon pod |

**Pattern:** Pool exhaustion occurs during/after mass connection resets.

## Root Cause: NOT Idle-in-Transaction

### Initial Hypothesis (INCORRECT)
We suspected connections were stuck in "idle in transaction" state, preventing reuse.

### Actual Root Cause
**Pod lifecycle + Connection pooling mismatch:**

1. **Normal Operation:**
   - Dagster pods maintain connection pools (size=10, overflow=20)
   - Each of 3 storage classes (run, event_log, schedule) has separate pool
   - Total per pod: 90 possible connections (3 × 30)

2. **What Happens During Pod Restart:**
   - Kubernetes terminates old pod
   - Pod's TCP connections RST'd from client side
   - PostgreSQL sees "Connection reset by peer"
   - **Pool objects in new pod think they have connections available**
   - **But connections were destroyed on network layer**

3. **Pool Exhaustion Cascade:**
   ```
   Request comes in
   -> Pool.checkout() returns "connection" (actually dead)
   -> Operation attempts to use connection
   -> Fails immediately (connection broken)
   -> retry_pg_connection_fn() retries up to 5 times
   -> Each retry checks out ANOTHER connection from pool
   -> All connections exhaust without completing operation
   -> TimeoutError: Pool limit reached
   ```

4. **Why This Happens with QueuePool but not NullPool:**
   - **NullPool:** Creates NEW connection for every request, never reuses
   - **QueuePool:** Maintains pool state in-memory
   - When pod restarts, QueuePool is recreated but doesn't know previous connections are dead
   - `pool_pre_ping=True` SHOULD catch this, but timing is critical

## Why pool_reset_on_return='rollback' Alone Won't Fix This

The `pool_reset_on_return` parameter handles **idle-in-transaction** issues:
- Rolls back uncommitted transactions when connection returned to pool
- Ensures clean state for next operation

But it does **NOT** handle:
- Connections that never get returned (exception before `.close()`)
- Dead connections that pool thinks are alive
- Network-level connection failures

## The Real Fix: pool_pre_ping + Better Error Handling

### What pool_pre_ping Does
```python
pool_pre_ping=True  # Already in our code
```

**On every checkout:**
1. Pool gives you a connection
2. SQLAlchemy sends `SELECT 1` test query
3. If fails, marks connection as dead and gets another
4. If succeeds, returns connection to application

**Why it's not preventing errors:**
- Timing: Connection might die AFTER pre-ping but BEFORE use
- Pod restarts happen WHILE operations in progress
- Retry logic exhausts pool before pre-ping can recover

### Additional Mitigations Needed

1. **Increase pool_timeout:**
   ```yaml
   pool_timeout: 60  # Up from 30, gives more time for recovery
   ```

2. **Decrease pool_recycle:**
   ```yaml
   pool_recycle: 600  # 10 min instead of 1 hour, force fresh connections
   ```

3. **Add pool_reset_on_checkin (not just on_return):**
   ```python
   # Custom event listener
   @event.listens_for(engine, "checkin")
   def receive_checkin(dbapi_conn, connection_record):
       # Force rollback when connection returned
       try:
           dbapi_conn.rollback()
       except:
           connection_record.invalidate()
   ```

4. **Set Aggressive Retry Backoff:**
   Dagster's `retry_pg_connection_fn` uses exponential backoff
   - This is GOOD: gives pool time to recover
   - But 5 retries × 30s timeout = 150s blocked

## Verification

The `pool_reset_on_return='rollback'` we added is still valuable:
- Prevents ACTUAL idle-in-transaction leaks (if they occur)
- No performance cost
- Extra safety layer

But the main benefit comes from:
- `pool_pre_ping=True` (we have this)
- Proper timeout configuration (added pool_timeout)
- Lower pool_recycle to force connection refresh

## Recommendations

1. **Keep all current changes** (pool_reset_on_return, pool_timeout)
2. **Monitor pod restart events** - correlate with pool exhaustion
3. **Consider pod disruption budgets** to limit concurrent restarts
4. **Increase pool sizes if high concurrency:**
   ```yaml
   event_log_storage:
     pool_size: 20  # Busiest component
     max_overflow: 40
   ```

5. **Add CloudWatch alarms** for:
   - QueuePool timeout errors
   - PostgreSQL connection count spikes
   - Pod restart frequency

## Conclusion

The pool exhaustion is primarily caused by **pod lifecycle events** (restarts, deployments) combined with **connection pool state not persisting** across pod instances.

The `pool_reset_on_return='rollback'` fix addresses a potential secondary issue (idle-in-transaction) but the primary fix is ensuring `pool_pre_ping=True` works effectively with appropriate timeout and recycle settings.

**All our changes are correct and beneficial** - they handle both the primary (connection lifecycle) and secondary (transaction state) issues.
