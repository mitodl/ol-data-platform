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
from sqlalchemy.dialects.mysql.mysqlconnector import MySQLDialect_mysqlconnector
from starrocks.dialect import StarRocksDialect


class StarRocksMySQLConnectorDialect(MySQLDialect_mysqlconnector, StarRocksDialect):
    name = "starrocks"
    supports_statement_cache = True
    statement_compiler = StarRocksDialect.statement_compiler
    ddl_compiler = StarRocksDialect.ddl_compiler
    type_compiler_cls = StarRocksDialect.type_compiler_cls
    preparer = StarRocksDialect.preparer
    # The MRO reaches these through the PyMySQL and MySQLdb dialects, which
    # assume a MySQLdb-style connection. Use the driver-neutral ones.
    on_connect = MySQLDialect.on_connect
    do_executemany = MySQLDialect.do_executemany
    description_encoding = None
