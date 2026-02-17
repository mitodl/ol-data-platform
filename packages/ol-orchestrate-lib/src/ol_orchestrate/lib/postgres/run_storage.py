"""Postgres run storage with connection pooling."""

from typing import Any

import sqlalchemy.pool as db_pool
from dagster._config import Field, IntSource
from dagster._config.config_schema import UserConfigSchema
from dagster._core.storage.config import PostgresStorageConfig, pg_config
from dagster._core.storage.runs import InstanceInfo, RunStorageSqlMetadata
from dagster._core.storage.sql import create_engine, stamp_alembic_rev
from dagster._serdes import ConfigurableClassData
from dagster_postgres.run_storage import PostgresRunStorage
from dagster_postgres.utils import (
    pg_alembic_config,
    pg_url_from_config,
    retry_pg_connection_fn,
    retry_pg_creation_fn,
    set_pg_statement_timeout,
)
from sqlalchemy import event, inspect


class PooledPostgresRunStorage(PostgresRunStorage):
    """Postgres-backed run storage with proper connection pooling.

    This is a drop-in replacement for PostgresRunStorage that uses QueuePool
    instead of NullPool to efficiently manage database connections and prevent
    connection exhaustion during complex Dagster jobs.

    Configuration in dagster.yaml:

    .. code-block:: yaml

        run_storage:
          module: ol_orchestrate.lib.postgres
          class: PooledPostgresRunStorage
          config:
            postgres_db:
              username:
                env: DAGSTER_PG_USERNAME
              password:
                env: DAGSTER_PG_PASSWORD
              hostname:
                env: DAGSTER_PG_HOST
              db_name:
                env: DAGSTER_PG_DB
              port: 5432
            pool_size: 10
            max_overflow: 20
            pool_recycle: 3600
    """

    def __init__(  # noqa: PLR0913
        self,
        postgres_url: str,
        should_autocreate_tables: bool = True,  # noqa: FBT001, FBT002
        inst_data: ConfigurableClassData | None = None,
        pool_size: int = 10,
        max_overflow: int = 20,
        pool_recycle: int = 3600,
    ) -> None:
        """Initialize PooledPostgresRunStorage.

        Args:
            postgres_url: PostgreSQL connection URL
            should_autocreate_tables: Whether to auto-create tables if missing
            inst_data: Dagster instance configuration data
            pool_size: Number of permanent connections in the pool
            max_overflow: Number of additional connections above pool_size
            pool_recycle: Recycle connections after this many seconds
        """
        self._inst_data = inst_data
        self.postgres_url = postgres_url
        self.should_autocreate_tables = should_autocreate_tables
        self._pool_size = pool_size
        self._max_overflow = max_overflow
        self._pool_recycle = pool_recycle

        # Use QueuePool instead of NullPool for efficient connection reuse
        self._engine = create_engine(
            self.postgres_url,
            isolation_level="AUTOCOMMIT",
            poolclass=db_pool.QueuePool,
            pool_size=self._pool_size,
            max_overflow=self._max_overflow,
            pool_recycle=self._pool_recycle,
            pool_pre_ping=True,
        )

        self._index_migration_cache: dict[Any, Any] = {}

        if self.should_autocreate_tables:
            table_names = retry_pg_connection_fn(
                lambda: inspect(self._engine).get_table_names()
            )
            if "runs" not in table_names:
                retry_pg_creation_fn(self._init_db)
                self.migrate()
                self.optimize()
            elif "instance_info" not in table_names:
                InstanceInfo.create(self._engine)

        super(PostgresRunStorage, self).__init__()

    def _init_db(self) -> None:
        """Initialize database tables."""
        with self.connect() as conn, conn.begin():
            RunStorageSqlMetadata.create_all(conn)
            stamp_alembic_rev(pg_alembic_config(__file__), conn)

    def optimize_for_webserver(
        self, statement_timeout: int, pool_recycle: int, max_overflow: int
    ) -> None:
        """Configure connection pooling for webserver use."""
        kwargs = {
            "isolation_level": "AUTOCOMMIT",
            "poolclass": db_pool.QueuePool,
            "pool_size": self._pool_size,
            "pool_recycle": pool_recycle,
            "max_overflow": max_overflow,
            "pool_timeout": self._pool_timeout,
            "pool_pre_ping": True,
        }

        existing_options = self._engine.url.query.get("options")
        if existing_options:
            kwargs["connect_args"] = {"options": existing_options}

        self._engine = create_engine(self.postgres_url, **kwargs)
        event.listen(
            self._engine,
            "connect",
            lambda connection, _: set_pg_statement_timeout(
                connection, statement_timeout
            ),
        )

    @classmethod
    def config_type(cls) -> UserConfigSchema:
        """Return configuration schema with pool configuration options."""
        return {
            **pg_config(),
            "pool_size": Field(
                IntSource,
                is_required=False,
                default_value=10,
                description="Number of connections in the pool",
            ),
            "max_overflow": Field(
                IntSource,
                is_required=False,
                default_value=20,
                description="Additional connections beyond pool_size",
            ),
            "pool_recycle": Field(
                IntSource,
                is_required=False,
                default_value=3600,
                description="Recycle connections after N seconds",
            ),
            "pool_timeout": Field(
                IntSource,
                is_required=False,
                default_value=30,
                description="Seconds to wait for connection from pool",
            ),
        }

    @classmethod
    def from_config_value(
        cls,
        inst_data: ConfigurableClassData | None,
        config_value: PostgresStorageConfig,
    ) -> "PooledPostgresRunStorage":
        """Create instance from configuration."""
        return cls(
            inst_data=inst_data,
            postgres_url=pg_url_from_config(config_value),
            should_autocreate_tables=bool(
                config_value.get("should_autocreate_tables", True)
            ),
            pool_size=int(config_value.get("pool_size", 10)),
            max_overflow=int(config_value.get("max_overflow", 20)),
            pool_recycle=int(config_value.get("pool_recycle", 3600)),
            pool_timeout=int(config_value.get("pool_timeout", 30)),
        )

    @property
    def inst_data(self) -> ConfigurableClassData | None:
        """Return instance configuration data."""
        return self._inst_data
