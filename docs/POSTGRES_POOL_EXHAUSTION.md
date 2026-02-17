# Connection Pool Exhaustion - Diagnosis and Solutions

## Symptom

```
WARNING:root:Retrying failed database connection: QueuePool limit of size 10 overflow 20 reached, connection timed out, timeout 30.00
```

## What This Means

All 30 connections (10 pool + 20 overflow) are in use and a new request waited 30 seconds for a connection but timed out.

## Root Cause

**Connection leaks** - connections are being acquired but not properly returned to the pool. This happens when:

1. **Long-running transactions** hold connections
2. **Unclosed transactions** after exceptions
3. **Too many concurrent operations** for the pool size
4. **Slow queries** blocking connections

## Immediate Solutions

### Solution 1: Increase Pool Size

Increase `pool_size` and `max_overflow`:

```yaml
run_storage:
  module: ol_orchestrate.lib.postgres
  class: PooledPostgresRunStorage
  config:
    postgres_db:
      # ... connection details ...
    pool_size: 20          # Increased from 10
    max_overflow: 40       # Increased from 20
    pool_recycle: 3600
    pool_timeout: 60       # Increased timeout
```

Apply to all three storage classes (`run_storage`, `event_log_storage`, `schedule_storage`).

**Total connections**: (20 + 40) × 3 = **180 max**

### Solution 2: Adjust Pool Timeout

Give operations more time to complete:

```yaml
pool_timeout: 60  # Wait 60 seconds instead of 30
```

### Solution 3: Decrease pool_recycle

Recycle connections more frequently to avoid stale connections:

```yaml
pool_recycle: 1800  # 30 minutes instead of 1 hour
```

## Diagnostic Commands

### Check Active Connections

```sql
-- Total connections to your database
SELECT count(*) as total_connections
FROM pg_stat_activity
WHERE datname = 'dagster';

-- Connections by state
SELECT state, count(*) as count
FROM pg_stat_activity
WHERE datname = 'dagster'
GROUP BY state
ORDER BY count DESC;

-- Long-running queries (potential blockers)
SELECT pid, now() - query_start as duration, state, query
FROM pg_stat_activity
WHERE datname = 'dagster'
  AND state = 'active'
  AND now() - query_start > interval '30 seconds'
ORDER BY duration DESC;

-- Idle in transaction (connection leaks!)
SELECT pid, now() - state_change as duration, state, query
FROM pg_stat_activity
WHERE datname = 'dagster'
  AND state = 'idle in transaction'
ORDER BY duration DESC;
```

### Check Postgres Max Connections

```sql
SHOW max_connections;

-- Current usage vs limit
SELECT
  (SELECT count(*) FROM pg_stat_activity) as current,
  (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max,
  (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') -
  (SELECT count(*) FROM pg_stat_activity) as available;
```

### Kill Stuck Connections

```sql
-- Kill specific connection
SELECT pg_terminate_backend(12345);  -- Replace with actual PID

-- Kill all idle in transaction (BE CAREFUL!)
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'dagster'
  AND state = 'idle in transaction'
  AND now() - state_change > interval '5 minutes';
```

## Advanced Debugging

### Enable SQLAlchemy Pool Logging

Add to your Dagster instance:

```python
# In your definitions.py or dagster.yaml
import logging
logging.getLogger('sqlalchemy.pool').setLevel(logging.DEBUG)
```

This logs every connection checkout/checkin.

### Check Pool Statistics

Add Python code to log pool stats:

```python
from dagster import get_dagster_logger

logger = get_dagster_logger()

# In your asset/op
def my_asset(context):
    # Access the storage instance
    storage = context.instance.run_storage
    pool = storage._engine.pool

    logger.info(f"Pool size: {pool.size()}")
    logger.info(f"Checked out: {pool.checkedout()}")
    logger.info(f"Overflow: {pool.overflow()}")
    logger.info(f"Checked in: {pool.checkedin()}")
```

## Long-Term Solutions

### 1. Optimize Query Performance

Slow queries hold connections longer:

```sql
-- Find slow queries
SELECT query, mean_exec_time, calls
FROM pg_stat_statements
WHERE mean_exec_time > 1000  -- > 1 second
ORDER BY mean_exec_time DESC
LIMIT 20;
```

Add indexes, optimize WHERE clauses, limit result sets.

### 2. Increase Postgres max_connections

Edit `postgresql.conf`:

```
max_connections = 300  # Increase from default 100
```

Restart Postgres after changing.

### 3. Use Connection Pooler (pgBouncer)

For very high connection counts, use pgBouncer:

```
application → pgBouncer (1000s of connections) → Postgres (100 connections)
```

### 4. Review Asset Concurrency

Limit concurrent asset materializations:

```python
@asset(
    op_tags={"dagster/concurrency_key": "database_ops"}
)
def my_asset():
    ...
```

Configure concurrency limits in dagster.yaml:

```yaml
concurrency:
  default_op_concurrency_limit: 10
  op_concurrency_limits:
    - key: "database_ops"
      limit: 5
```

### 5. Batch Operations

Instead of many small DB operations, batch them:

```python
# Bad: 1000 connections
for item in items:
    storage.write(item)

# Good: 1 connection
storage.write_batch(items)
```

## Recommended Settings by Deployment Size

### Small (Development)
```yaml
pool_size: 5
max_overflow: 10
pool_timeout: 30
pool_recycle: 1800
```
Total: 45 connections max (3 storage × 15)

### Medium (Production - Low Traffic)
```yaml
pool_size: 10
max_overflow: 20
pool_timeout: 60
pool_recycle: 1800
```
Total: 90 connections max (3 storage × 30)

### Large (Production - High Traffic)
```yaml
pool_size: 20
max_overflow: 40
pool_timeout: 90
pool_recycle: 1800
```
Total: 180 connections max (3 storage × 60)

### Extra Large (Multi-Tenant)
```yaml
pool_size: 30
max_overflow: 50
pool_timeout: 120
pool_recycle: 900
```
Total: 240 connections max (3 storage × 80)

**Remember**: Multiply by number of Dagster instances (webserver pods + daemon pods).

## Monitoring & Alerts

Set up alerts for:

1. **Connection usage > 80%**: `(current_connections / max_connections) > 0.8`
2. **Long-running transactions**: Queries > 5 minutes
3. **Idle in transaction**: Connections idle in transaction > 2 minutes
4. **Pool timeouts**: Log entries with "QueuePool limit"

## Common Mistakes

❌ **Setting pool_size too high** → Overwhelms Postgres
✅ **Start conservative, increase based on monitoring**

❌ **Ignoring "idle in transaction"** → Connection leaks
✅ **Monitor and kill stuck transactions**

❌ **Not recycling connections** → Stale connection errors
✅ **Set pool_recycle to 30-60 minutes**

❌ **Same pool size for all storage types** → Event log needs more
✅ **Size event_log_storage pool higher**

## Quick Checklist

- [ ] Check `pg_stat_activity` for stuck connections
- [ ] Verify Postgres `max_connections` is sufficient
- [ ] Increase `pool_size` and `max_overflow`
- [ ] Increase `pool_timeout` to give operations more time
- [ ] Enable pool logging for debugging
- [ ] Review slow queries and add indexes
- [ ] Set up monitoring for connection usage
- [ ] Kill any "idle in transaction" connections
- [ ] Consider pgBouncer for very high scale

## Updated Configuration

After adding `pool_timeout`, your configuration should include:

```yaml
run_storage:
  module: ol_orchestrate.lib.postgres
  class: PooledPostgresRunStorage
  config:
    postgres_db:
      username: {env: DAGSTER_PG_USERNAME}
      password: {env: DAGSTER_PG_PASSWORD}
      hostname: {env: DAGSTER_PG_HOST}
      db_name: {env: DAGSTER_PG_DB}
      port: 5432
    pool_size: 20
    max_overflow: 40
    pool_recycle: 1800
    pool_timeout: 60       # NEW PARAMETER
```

Apply the same to `event_log_storage` and `schedule_storage`.
