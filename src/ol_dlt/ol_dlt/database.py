"""Reusable relational-database source for ol_dlt pipelines.

One declarative spec per source database produces a ``@dlt.source`` whose
resources are named with the warehouse's raw-table convention
(``raw__<system>__app__postgres__<table>``), so a new database source is a
data-only change: declare the tables, wrap it in the ``data_loading`` code
location, done.

Environment alignment
    The database a pipeline reads is chosen by the active ``DLT_PROFILE``
    (which ``data_loading`` derives from ``DAGSTER_ENVIRONMENT``): the QA
    Dagster deployment reads the QA database, production reads production, and
    ``dev``/``ci``/``test`` read the local-dev CloudNativePG cluster that
    ``ol-infrastructure/local-dev`` stands up — so a developer never needs
    access to a deployed environment to exercise a pipeline.

Credentials
    Deployed profiles fetch short-lived credentials from the Vault database
    secrets engine (``<mount>/creds/readonly``) at *connection* time, via a
    SQLAlchemy ``do_connect`` hook. Resolving them there rather than when the
    source is constructed matters: Dagster instantiates every source at code-
    location import, and a lease taken then would be stale (or revoked) by the
    time a scheduled run actually executes.

Reflection is deferred (``defer_table_reflect=True``) for the same reason — no
database connection is opened until a resource is extracted.
"""

import logging
from collections.abc import Generator, Sequence
from dataclasses import dataclass, field
from typing import Any

import dlt
from dlt.sources.sql_database import sql_table
from sqlalchemy import event
from sqlalchemy.engine import URL, Engine

from ol_dlt import config, vault

logger = logging.getLogger(__name__)

# Rows per batch pulled from the source. The pyarrow backend materializes a
# batch in memory, so this bounds peak memory per table.
DEFAULT_CHUNK_SIZE = 50_000

# Matches the shared CloudNativePG cluster in ol-infrastructure/local-dev
# (local-dev/infra/modules/database.py). Reach it with:
#   kubectl port-forward -n local-infra svc/local-pg-rw 5432:5432
LOCAL_DEV_HOST = "localhost"
LOCAL_DEV_USERNAME = "app"
LOCAL_DEV_PASSWORD = "localdev"  # noqa: S105  # pragma: allowlist secret


@dataclass(frozen=True)
class DatabaseTable:
    """One source table to ingest.

    Args:
        name: Table name in the source schema.
        primary_key: Column(s) uniquely identifying a row. Required when
            ``cursor_column`` is set, since incremental loads merge on it.
        cursor_column: Monotonic column driving incremental loads. When unset
            the table is fully re-read and replaced each run — the right
            default for small tables with no reliable modification timestamp.
        excluded_columns: Columns never to select. Use this for secrets and
            credential material, which must not reach the warehouse at all.
    """

    name: str
    primary_key: str | Sequence[str] | None = None
    cursor_column: str | None = None
    excluded_columns: Sequence[str] = ()

    def __post_init__(self) -> None:
        """Reject an incremental table that has no key to merge on."""
        if self.cursor_column and not self.primary_key:
            msg = (
                f"{self.name}: primary_key is required when cursor_column is set "
                "(incremental loads merge on the primary key)"
            )
            raise ValueError(msg)


@dataclass(frozen=True)
class DatabaseSourceSpec:
    """Declarative description of one source database.

    Args:
        name: Short source name. Drives the dlt pipeline name, the per-source
            destination prefix, and the ``<NAME>_DB_*`` environment variables.
        raw_table_prefix: Prefix applied to each table name to form the raw
            warehouse table (and Dagster asset) name.
        database: Database name to connect to.
        vault_mount: Vault database secrets-engine mount serving this database,
            e.g. ``postgres-keycloak``. Used by deployed profiles only.
        tables: The tables to ingest.
        db_schema: Schema the tables live in, or ``None`` for engines that have
            no named schema.
        vault_role: Vault role to request. Ingestion is read-only, always.
        port: Database port.
        drivername: SQLAlchemy driver.
    """

    name: str
    raw_table_prefix: str
    database: str
    vault_mount: str
    tables: Sequence[DatabaseTable] = field(default_factory=tuple)
    db_schema: str | None = "public"
    vault_role: str = "readonly"
    port: int = 5432
    drivername: str = "postgresql+psycopg2"

    @property
    def env_prefix(self) -> str:
        """Environment-variable prefix for per-deployment overrides."""
        return f"{self.name.upper()}_DB"

    def raw_table_name(self, table: DatabaseTable) -> str:
        """Return the raw warehouse table name for ``table``."""
        return f"{self.raw_table_prefix}{table.name}"


def _uses_vault(profile: str) -> bool:
    """Return True when the profile reads a deployed (Vault-backed) database."""
    return profile in config.ICEBERG_PROFILES


def _connection_url(spec: DatabaseSourceSpec, profile: str) -> URL:
    """Build the connection URL for ``spec`` under ``profile``.

    Deployed profiles omit the username/password — those are injected per
    connection from Vault — and require TLS, which the RDS parameter group
    enforces anyway (``rds.force_ssl``).
    """
    host = config.resolve_secret(None, f"{spec.env_prefix}_HOST")
    port = config.resolve_secret(None, f"{spec.env_prefix}_PORT")
    if _uses_vault(profile):
        if not host:
            msg = (
                f"{spec.env_prefix}_HOST is not set. The {profile} deployment must "
                f"pass the {spec.name} database host through from its Pulumi stack "
                "reference."
            )
            raise ValueError(msg)
        return URL.create(
            spec.drivername,
            host=host,
            port=int(port) if port else spec.port,
            database=spec.database,
            query={"sslmode": "require"},
        )
    return URL.create(
        spec.drivername,
        username=config.resolve_secret(None, f"{spec.env_prefix}_USERNAME")
        or LOCAL_DEV_USERNAME,
        password=config.resolve_secret(None, f"{spec.env_prefix}_PASSWORD")
        or LOCAL_DEV_PASSWORD,
        host=host or LOCAL_DEV_HOST,
        port=int(port) if port else spec.port,
        database=spec.database,
    )


def _vault_credential_injector(spec: DatabaseSourceSpec) -> Any:  # noqa: ANN401
    """Return an engine adapter that supplies Vault credentials per connection.

    SQLAlchemy's ``do_connect`` event fires when a pooled connection is
    actually opened, so each connection gets credentials that are live *then*
    rather than whenever the pipeline was defined.
    """

    def adapt(engine: Engine) -> Engine:
        @event.listens_for(engine, "do_connect")
        def _inject(
            _dialect: Any,  # noqa: ANN401
            _conn_rec: Any,  # noqa: ANN401
            _cargs: Any,  # noqa: ANN401
            cparams: dict[str, Any],
        ) -> None:
            username, password = vault.read_database_credentials(
                spec.vault_mount, spec.vault_role
            )
            cparams["user"] = username
            cparams["password"] = password

        return engine

    return adapt


def build_table_resource(
    spec: DatabaseSourceSpec,
    table: DatabaseTable,
    *,
    profile: str | None = None,
) -> Any:  # noqa: ANN401
    """Return the dlt resource for one table of ``spec``."""
    resolved_profile = profile or config.active_profile()
    raw_name = spec.raw_table_name(table)
    incremental = (
        dlt.sources.incremental(table.cursor_column) if table.cursor_column else None
    )
    resource = sql_table(
        credentials=_connection_url(spec, resolved_profile).render_as_string(
            hide_password=False
        ),
        table=table.name,
        schema=spec.db_schema,
        incremental=incremental,
        chunk_size=DEFAULT_CHUNK_SIZE,
        # pyarrow reflects source types faithfully instead of re-inferring them
        # from sampled rows, which keeps the Iceberg schema stable run to run.
        backend="pyarrow",
        reflection_level="full",
        # No connection until extraction — see the module docstring.
        defer_table_reflect=True,
        excluded_columns=list(table.excluded_columns) or None,
        write_disposition="merge" if table.cursor_column else "replace",
        primary_key=table.primary_key,
        engine_adapter_callback=(
            _vault_credential_injector(spec) if _uses_vault(resolved_profile) else None
        ),
    )
    return resource.with_name(raw_name).apply_hints(
        table_name=raw_name,
        table_format=config.active_table_format(resolved_profile),
    )


def build_database_source(
    spec: DatabaseSourceSpec,
    *,
    tables: Sequence[str] | None = None,
    profile: str | None = None,
) -> Any:  # noqa: ANN401
    """Return the ``@dlt.source`` for ``spec``.

    Args:
        spec: The database source description.
        tables: Optional subset of ``spec.tables`` names to include, for
            targeted local runs.
        profile: Override the active profile (mainly for tests).
    """
    if tables is None:
        selected = list(spec.tables)
    else:
        wanted = set(tables)
        selected = [table for table in spec.tables if table.name in wanted]
    if not selected:
        msg = f"{spec.name}: no tables selected from {tables!r}"
        raise ValueError(msg)

    @dlt.source(name=f"{spec.name}_ingest")
    def _source() -> Generator[Any]:
        for table in selected:
            yield build_table_resource(spec, table, profile=profile)

    return _source()


def pipeline_for(spec: DatabaseSourceSpec) -> dlt.Pipeline:
    """Return the dlt pipeline for ``spec``'s destination."""
    return config.pipeline_for(spec.name)
