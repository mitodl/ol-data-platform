"""StarRocks database resource with Vault dynamic credentials.

Modeled on VaultMySQLClientFactory (legacy_openedx/resources/mysql_db.py):
credentials come from Vault's database secrets engine, generated fresh on each
connection -- no passwords are held in Dagster config.

The exception is ``static_username``, for a StarRocks that has no Vault mount
(the local-dev cluster in ol-infrastructure, whose root takes no password).
"""

import logging
import ssl
import time
from typing import Any

from dagster import ConfigurableResource, ResourceDependency
from ol_orchestrate.resources.secrets.vault import Vault
from pydantic import Field as PydanticField
from pymysql import Connection, connect
from pymysql.cursors import DictCursor
from pymysql.err import OperationalError

log = logging.getLogger(__name__)

DEFAULT_STARROCKS_PORT = 9030


def _make_ssl_context() -> ssl.SSLContext:
    # Matches bin/starrocks-auth's `--ssl --ssl-verify-server-cert=OFF` mysql
    # invocation and legacy_openedx/resources/mysql_db.py's MySQLClient:
    # encrypted transport, no cert verification (internal cluster cert).
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


# MySQL-wire-protocol error codes that warrant a fresh Vault credential + retry.
# 1044 ER_DBACCESS_DENIED_ERROR / 1045 ER_ACCESS_DENIED_ERROR - the dynamic user
#   Vault just created hasn't propagated across StarRocks FE nodes yet.
# 2003 CR_CONN_HOST_ERROR - the connect() below failed outright, which is what
#   an FE rolling restart looks like from here. Every attempt opens its own
#   connection, so this is reachable on all of them, not just the first.
# 2006 CR_SERVER_GONE_ERROR / 2013 CR_SERVER_LOST - dropped connection.
_RETRIABLE_ERRORS: frozenset[int] = frozenset({1044, 1045, 2003, 2006, 2013})
_MAX_ATTEMPTS = 3
_RETRY_BASE_DELAY = 1  # seconds; doubles each attempt

# A login Vault just created is written on the leader FE and reaches the
# followers through journal replay, so for a short while some FEs refuse it.
_LOGIN_NOT_REPLAYED: frozenset[int] = frozenset({1044, 1045})
# How long `wait_for_login` waits for every FE to accept a new login. Vault
# propagation resolves well inside 30s (lib/starrocks_dbt.py's retry notes).
LOGIN_PROPAGATION_TIMEOUT_SECONDS = 120
_LOGIN_POLL_INTERVAL_SECONDS = 2
# Read/write timeout on each readiness probe, so an FE that stalls mid-handshake
# or mid-query can't hold the wait past its deadline.
_LOGIN_PROBE_TIMEOUT_SECONDS = 10
# What `_fe_refusal` returns for an FE it couldn't connect to at all.
_UNREACHABLE = "unreachable"


class LoginNotPropagatedError(Exception):
    """A new login was still refused by some FE when the wait ran out."""

    def __init__(self, username: str, refusing: dict[str, str]) -> None:
        self.refusing = refusing
        detail = "; ".join(f"{fe}: {error}" for fe, error in sorted(refusing.items()))
        super().__init__(
            f"StarRocks login {username} was not accepted by every FE within "
            f"{LOGIN_PROPAGATION_TIMEOUT_SECONDS}s: {detail}"
        )


class StarRocksResource(ConfigurableResource["StarRocksResource"]):
    """Executes statements against StarRocks over its MySQL wire protocol port."""

    vault: ResourceDependency[Vault]
    vault_mount_point: str = PydanticField(
        description=(
            "Vault database secrets engine mount point for StarRocks, "
            "e.g. 'database-starrocks'. Not environment-specific -- each "
            "environment runs its own separate Vault deployment."
        )
    )
    vault_role: str = PydanticField(
        default="admin",
        description="Vault database role to generate credentials for.",
    )
    host: str = PydanticField(description="StarRocks FE host name.")
    port: int = PydanticField(
        default=DEFAULT_STARROCKS_PORT,
        description="StarRocks MySQL-protocol query port.",
    )
    database: str = PydanticField(
        description="StarRocks database/schema to connect to, e.g. 'b2b_analytics'."
    )
    static_username: str | None = PydanticField(
        default=None,
        description=(
            "Log in as this user with static_password and never call Vault. "
            "Only for a StarRocks with no Vault mount, i.e. the local-dev "
            "cluster. The connection then does not require TLS."
        ),
    )
    static_password: str = PydanticField(
        default="",
        description="Password for static_username. The local-dev root has none.",
    )

    def generate_credentials(self) -> tuple[str, str]:
        """Return (username, password): fresh from Vault, or the static login.

        Public so callers that need StarRocks credentials outside of `execute()`
        (e.g. injecting DBT_STARROCKS_USERNAME/PASSWORD for a dbt CLI invocation)
        can reuse the same Vault database secrets engine call.
        """
        if self.static_username is not None:
            return self.static_username, self.static_password
        creds = self.vault.client.secrets.database.generate_credentials(
            mount_point=self.vault_mount_point,
            name=self.vault_role,
        )["data"]
        return creds["username"], creds["password"]

    def connect(self) -> Connection[DictCursor]:
        """Open a connection with fresh credentials and no retry.

        For a caller that runs many statements and would otherwise have Vault
        create a database user for each one, as `execute` and `fetch` do.
        """
        username, password = self.generate_credentials()
        return self._connect_as(
            self.host, self.port, username, password, database=self.database
        )

    def _connect_as(  # noqa: PLR0913
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        *,
        database: str | None = None,
        timeout: int | None = None,
    ) -> Connection[DictCursor]:
        return connect(
            host=host,
            port=port,
            database=database,
            user=username,
            password=password,
            cursorclass=DictCursor,
            # An explicit context makes pymysql refuse a server without TLS,
            # which the static login must not require.
            ssl=None if self.static_username is not None else _make_ssl_context(),
            # pymysql's own defaults: a 10s connect timeout, no read/write one.
            connect_timeout=timeout or 10,
            read_timeout=timeout,
            write_timeout=timeout,
        )

    def wait_for_login(self, username: str, password: str) -> None:
        """Return once every reachable FE accepts *username*, a login Vault just made.

        dbt connects through the round-robin FE service, so a login that one FE
        has not replayed yet fails whichever dbt node lands there with a 1045.
        Checking each FE directly, by the address SHOW FRONTENDS gives, turns
        that into a short wait before the spawn, not a failed node mid-build.
        The static login exists on every FE already and is not checked.

        Vault creates the login in separate statements (CREATE USER, GRANT
        <role>, ALTER USER ... DEFAULT ROLE <role>), and a follower replays
        them in order, so it can accept the password before the role is
        active. An FE is ready only once ``is_role_in_session(vault_role)`` is
        true there; every Vault role grants the StarRocks role of the same name
        as its default.

        The FE list is read again on every poll. An FE that refuses the
        connection itself (2003 and the other connection errors, including a
        probe that times out) is skipped: the service routes only to Ready FEs,
        so dbt can't land there either, and during a rolling restart an FE that
        is down must not hold the wait open past its deadline. 1044/1045 or an
        inactive role on a reachable FE means the login hasn't replayed there
        yet. Any other error is raised.

        :raises LoginNotPropagatedError: when some reachable FE still refuses the
            login, or SHOW FRONTENDS still can't be read, after
            LOGIN_PROPAGATION_TIMEOUT_SECONDS.
        """
        if self.static_username is not None:
            return
        deadline = time.monotonic() + LOGIN_PROPAGATION_TIMEOUT_SECONDS
        while True:
            refusing = self._login_refusals(username, password, deadline)
            if not refusing:
                return
            # Every poll starts before the deadline, so a probe is skipped only
            # when the ones before it in the same poll ran past it.
            if time.monotonic() + _LOGIN_POLL_INTERVAL_SECONDS >= deadline:
                raise LoginNotPropagatedError(username, refusing)
            time.sleep(_LOGIN_POLL_INTERVAL_SECONDS)

    def _probe(
        self, host: str, port: int, username: str, password: str
    ) -> Connection[DictCursor]:
        return self._connect_as(
            host, port, username, password, timeout=_LOGIN_PROBE_TIMEOUT_SECONDS
        )

    def _role_active(self, conn: Connection[DictCursor]) -> bool:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT is_role_in_session(%s) AS active", (self.vault_role,)
            )
            rows = cursor.fetchall()
        return bool(rows[0]["active"])

    def _login_refusals(
        self, username: str, password: str, deadline: float
    ) -> dict[str, str]:
        """Map each FE (or the service) not yet ready for the login to why."""
        frontends = self._live_frontends(username, password)
        if isinstance(frontends, str):
            return {self.host: frontends}
        refusing: dict[str, str] = {}
        verified = 0
        for host, port in frontends:
            fe = f"{host}:{port}"
            if time.monotonic() >= deadline:
                refusing[fe] = "not checked before the deadline"
            elif (reason := self._fe_refusal(host, port, username, password)) is None:
                verified += 1
            elif reason != _UNREACHABLE:
                refusing[fe] = reason
        if not verified and not refusing:
            # Every FE was skipped as unreachable, yet the service answered. The
            # FE names (from the headless fe-search service, which publishes
            # not-ready pods) don't resolve or connect from here, so nothing has
            # been checked. Fail rather than pass without verifying anything.
            refusing[self.host] = (
                "no FE listed by SHOW FRONTENDS was reachable directly"
            )
        return refusing

    def _live_frontends(
        self, username: str, password: str
    ) -> list[tuple[str, int]] | str:
        """(host, query port) of every live FE, or why the service can't say yet."""
        try:
            conn = self._probe(self.host, self.port, username, password)
            try:
                # SHOW FRONTENDS needs the role's privileges, so check it first.
                if not self._role_active(conn):
                    return f"role {self.vault_role} is not active yet"
                with conn.cursor() as cursor:
                    cursor.execute("SHOW FRONTENDS")
                    rows = cursor.fetchall()
            finally:
                conn.close()
        except OperationalError as exc:
            if exc.args[0] not in _RETRIABLE_ERRORS:
                raise
            return str(exc)
        frontends = [
            (row["IP"], int(row["QueryPort"])) for row in rows if row["Alive"] == "true"
        ]
        # The service answered, so some FE is up. An empty list means the
        # columns changed shape, and the check would otherwise pass vacuously.
        return frontends or "SHOW FRONTENDS listed no live FE"

    def _fe_refusal(
        self, host: str, port: int, username: str, password: str
    ) -> str | None:
        """Why one FE isn't ready for the login, None when it is, or _UNREACHABLE."""
        try:
            conn = self._probe(host, port, username, password)
            try:
                active = self._role_active(conn)
            finally:
                conn.close()
        except OperationalError as exc:
            if exc.args[0] in _LOGIN_NOT_REPLAYED:
                return str(exc)
            if exc.args[0] not in _RETRIABLE_ERRORS:
                raise
            log.info("Skipping unreachable StarRocks FE %s:%s: %s", host, port, exc)
            return _UNREACHABLE
        return None if active else f"role {self.vault_role} is not active yet"

    def execute(self, sql: str, *, idempotent: bool = True) -> None:
        """Run *sql*, retrying with fresh credentials on a transient error.

        A fresh set of dynamic credentials is generated on every attempt (not just
        the first) since a 1044/1045 is most often caused by the previous attempt's
        just-created user not yet being visible on the FE node we connect to --
        generating a new one and retrying gives replication another round to catch
        up rather than reusing credentials known to be affected.

        Pass ``idempotent=False`` for a statement that cannot be re-run after
        dying partway, such as CREATE TABLE AS SELECT: an FE lost mid-statement
        can leave the table created, so a retry fails on "already exists" and
        hides the original error. Such a statement is still retried when the
        connection itself fails, since then it never started.
        """
        self._run(sql, retry_statement=idempotent)

    def fetch(
        self, sql: str, params: tuple[str, ...] | None = None
    ) -> list[dict[str, Any]]:
        """Run *sql* and return its rows, with `execute`'s credential retry.

        *params* are passed through to the driver's own placeholder
        substitution (`%s`) rather than interpolated into *sql*.

        The default is None rather than an empty tuple because pymysql applies
        `query % args` for any args that is not None -- including `()`, which
        binds nothing but still makes a literal `%` in *sql* (a `LIKE '%x%'`,
        a `date_format` pattern) raise "not enough arguments for format
        string". Statements with no placeholders have to skip the binding
        rather than pass an empty one. `execute` already passes None.

        Retrying a SELECT is unconditionally safe. A retry only happens when
        the previous attempt failed to connect or died mid-statement.
        """
        return self._run(sql, params) or []

    def _run(
        self,
        sql: str,
        params: tuple[str, ...] | None = None,
        *,
        retry_statement: bool = True,
    ) -> list[dict[str, Any]] | None:
        last_exc: OperationalError | None = None
        for attempt in range(_MAX_ATTEMPTS):
            if attempt:
                delay = _RETRY_BASE_DELAY * (2 ** (attempt - 1))
                log.warning(
                    "StarRocks error %s (attempt %d/%d) -- retrying in %ds with "
                    "fresh credentials",
                    last_exc,
                    attempt,
                    _MAX_ATTEMPTS,
                    delay,
                )
                time.sleep(delay)

            try:
                conn = self.connect()
            except OperationalError as exc:
                if exc.args[0] not in _RETRIABLE_ERRORS:
                    raise
                last_exc = exc
                continue

            try:
                with conn.cursor() as cursor:
                    cursor.execute(sql, params)
                    rows = cursor.fetchall()
                conn.commit()
            except OperationalError as exc:
                if not retry_statement or exc.args[0] not in _RETRIABLE_ERRORS:
                    raise
                last_exc = exc
                continue
            else:
                return list(rows)
            finally:
                conn.close()

        raise last_exc  # type: ignore[misc]  # loop always sets last_exc before falling through
