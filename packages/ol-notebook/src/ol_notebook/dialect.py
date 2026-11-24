"""The StarRocks SQLAlchemy dialect over mysql-connector-python.

The ``starrocks`` package's dialect is built on PyMySQL, which has no
``authentication_openid_connect_client`` plugin, so it can't log in with a
Keycloak token. mysql-connector-python can, but the PyMySQL dialect calls
MySQLdb-style methods on the connection (``character_set_name()`` at connect).
This takes the driver hooks from SQLAlchemy's mysqlconnector dialect and the
SQL compilation and reflection from StarRocks'. The name stays ``starrocks``
because marimo's data sources panel only lists catalogs for that dialect name.

Registered as ``starrocks+mysqlconnector`` through the ``sqlalchemy.dialects``
entry point in this package's pyproject.toml.
"""

from sqlalchemy.dialects.mysql.base import MySQLDialect
from sqlalchemy.dialects.mysql.mysqlconnector import (
    IdentifierPreparerCommon_mysqlconnector,
    MySQLCompiler_mysqlconnector,
    MySQLDialect_mysqlconnector,
)
from sqlalchemy.dialects.mysql.types import BIT
from starrocks.dialect import (
    StarRocksDialect,
    StarRocksIdentifierPreparer,
    StarRocksSQLCompiler,
)


# The PyMySQL preparer and compiler double every % for the driver's format
# paramstyle, but mysql-connector-python only interpolates when a query has
# parameters, so a parameterless `LIKE 'a%'` or `7 % 3` reached StarRocks as
# `%%`. These take mysqlconnector's percent handling over StarRocks' SQL.
class StarRocksMySQLConnectorIdentifierPreparer(
    IdentifierPreparerCommon_mysqlconnector, StarRocksIdentifierPreparer
):
    pass


class StarRocksMySQLConnectorCompiler(StarRocksSQLCompiler):
    visit_mod_binary = MySQLCompiler_mysqlconnector.visit_mod_binary


class StarRocksMySQLConnectorDialect(MySQLDialect_mysqlconnector, StarRocksDialect):
    name = "starrocks"
    supports_statement_cache = True
    statement_compiler = StarRocksMySQLConnectorCompiler
    ddl_compiler = StarRocksDialect.ddl_compiler
    preparer = StarRocksMySQLConnectorIdentifierPreparer
    colspecs = StarRocksDialect.colspecs | {
        BIT: MySQLDialect_mysqlconnector.colspecs[BIT]
    }
    # The MRO reaches these through the PyMySQL and MySQLdb dialects, which
    # assume a MySQLdb-style connection. Use the driver-neutral ones.
    on_connect = MySQLDialect.on_connect
    do_executemany = MySQLDialect.do_executemany
    description_encoding = None
