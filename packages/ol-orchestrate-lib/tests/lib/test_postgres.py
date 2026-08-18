"""Tests for ol_orchestrate.lib.postgres.

These subclasses exist for one reason: dagster_postgres builds every engine with
NullPool, so a process opens and closes a TCP connection per operation with no
reuse and no upper bound. On 2026-08-18 the data-production daemon sustained 331
connects/second that way and consumed all 28232 ephemeral ports in its network
namespace, after which psycopg2 failed every new connection with "Cannot assign
requested address" -- which reads as if PgBouncer were down, but is raised by
bind() before a packet leaves the pod.

So the property worth pinning is not storage behaviour, which is
dagster_postgres' own concern and is covered upstream. It is that each storage
ends up on a QueuePool whose retained size is the configured one, because
connections above pool_size are closed when returned rather than kept, and a
pool_size below a process's real concurrency reintroduces exactly the churn
these classes exist to remove.

Nothing here touches a database. SQLAlchemy creates engines lazily, so with
should_autocreate_tables=False the pool can be inspected without a connection
ever being opened.
"""

from unittest import mock

import pytest
from ol_orchestrate.lib.postgres import (
    PooledPostgresEventLogStorage,
    PooledPostgresRunStorage,
    PooledPostgresScheduleStorage,
)
from sqlalchemy.pool import QueuePool

POSTGRES_URL = (
    "postgresql://dagster:dagster@localhost:5432/dagster"  # pragma: allowlist secret
)

STORAGE_CLASSES = [
    PooledPostgresEventLogStorage,
    PooledPostgresRunStorage,
    PooledPostgresScheduleStorage,
]

# The three classes are configured from one dagster.yaml and are deployed
# together, so every property below is asserted against all three. A pooled
# event log next to a NullPool run storage would leave the churn in place.
parametrize_storages = pytest.mark.parametrize(
    "storage_class", STORAGE_CLASSES, ids=lambda cls: cls.__name__
)


def build_storage(storage_class, **pool_kwargs):
    """Construct a storage without letting it reach a database."""
    return storage_class(
        postgres_url=POSTGRES_URL,
        should_autocreate_tables=False,
        **pool_kwargs,
    )


@parametrize_storages
def test_storage_pools_connections_instead_of_reopening_them(storage_class) -> None:
    """The whole point: NullPool reconnects per operation, QueuePool reuses."""
    storage = build_storage(storage_class)

    assert isinstance(storage._engine.pool, QueuePool)


@parametrize_storages
def test_configured_pool_size_is_the_number_of_connections_retained(
    storage_class,
) -> None:
    """Connections above pool_size are closed on return, so this is the knob
    that decides how much churn survives -- it has to be what was asked for.
    """
    storage = build_storage(storage_class, pool_size=7, max_overflow=3)

    assert storage._engine.pool.size() == 7
    assert storage._engine.pool._max_overflow == 3


@parametrize_storages
def test_pool_recycle_and_timeout_reach_the_pool(storage_class) -> None:
    """pool_recycle bounds how long a connection outlives a PgBouncer restart;
    pool_timeout bounds how long a caller blocks on a saturated pool.
    """
    storage = build_storage(storage_class, pool_recycle=900, pool_timeout=15)

    assert storage._engine.pool._recycle == 900
    assert storage._engine.pool._timeout == 15


@parametrize_storages
def test_pool_timeout_defaults_to_failing_fast(storage_class) -> None:
    """A saturated pool has to raise in seconds. The config schema's default and
    the from_config_value fallback are written in two different places, and a
    disagreement between them is invisible until a pool actually saturates --
    at which point the wrong value turns a fast error into a process that hangs.
    """
    schema_default = storage_class.config_type()["pool_timeout"].default_value

    assert schema_default == 30
    assert build_storage(storage_class)._engine.pool._timeout == 30


@parametrize_storages
def test_config_schema_defaults_match_the_constructor_defaults(storage_class) -> None:
    """Dagster materializes schema defaults before from_config_value sees them,
    so the schema is what actually ships. Divergence means the documented
    default and the deployed one are different numbers.
    """
    config_type = storage_class.config_type()
    storage = build_storage(storage_class)

    assert config_type["pool_size"].default_value == storage._engine.pool.size()
    assert (
        config_type["max_overflow"].default_value == storage._engine.pool._max_overflow
    )
    assert config_type["pool_recycle"].default_value == storage._engine.pool._recycle


@parametrize_storages
def test_pool_settings_survive_loading_from_dagster_yaml(storage_class) -> None:
    """The deployed path is from_config_value, not the constructor -- a class
    whose __init__ pools correctly but drops the config on the floor would look
    fine everywhere except production.
    """
    storage = storage_class.from_config_value(
        None,
        {
            "postgres_db": {
                "username": "dagster",
                "password": "dagster",  # pragma: allowlist secret
                "hostname": "localhost",
                "db_name": "dagster",
                "port": 5432,
            },
            "should_autocreate_tables": False,
            "pool_size": 4,
            "max_overflow": 6,
            "pool_recycle": 120,
            "pool_timeout": 5,
        },
    )

    assert storage._engine.pool.size() == 4
    assert storage._engine.pool._max_overflow == 6
    assert storage._engine.pool._recycle == 120
    assert storage._engine.pool._timeout == 5


@parametrize_storages
def test_optimize_for_webserver_keeps_pooling(storage_class) -> None:
    """The webserver rebuilds the engine at startup to apply a statement
    timeout. It must not land back on the unpooled default.
    """
    storage = build_storage(storage_class, pool_size=6)

    storage.optimize_for_webserver(
        statement_timeout=5000, pool_recycle=60, max_overflow=2
    )

    assert isinstance(storage._engine.pool, QueuePool)
    assert storage._engine.pool.size() == 6
    assert storage._engine.pool._max_overflow == 2


@parametrize_storages
def test_optimize_for_webserver_disposes_the_engine_it_replaces(storage_class) -> None:
    """Rebuilding the engine drops the reference to the old one. Under NullPool
    that costs nothing because it holds no connections, but a QueuePool that is
    never disposed keeps its checked-in connections open for the life of the
    process -- pinning PgBouncer server connections nothing will ever use again.
    """
    storage = build_storage(storage_class)
    replaced_engine = storage._engine

    with mock.patch.object(
        replaced_engine, "dispose", wraps=replaced_engine.dispose
    ) as dispose:
        storage.optimize_for_webserver(
            statement_timeout=5000, pool_recycle=60, max_overflow=2
        )

    dispose.assert_called_once()
    assert storage._engine is not replaced_engine
