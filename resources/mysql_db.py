# -*- coding: utf-8 -*-

from typing import Dict, List, Text

import MySQLdb
from dagster import Field, Int, SolidExecutionContext, String, resource
from MySQLdb.cursors import DictCursor
from pypika import Query

DEFAULT_MYSQL_PORT = 3306


@resource(
    config={
        'mysql_hostname': Field(
            String,
            is_required=True,
            description='Host string for MySQL/MariaDB server'
        ),
        'mysql_port': Field(
            Int,
            is_required=False,
            default_value=DEFAULT_MYSQL_PORT,
            description='TCP Port number for MySQL/MariaDB server'
        ),
        'mysql_username': Field(
            String,
            is_required=True,
            description='Username for authenticating to MySQL/MariaDB server'
        ),
        'mysql_password': Field(
            String,
            is_required=False,
            description='Password for authenticating to MySQL/MariaDB server'
        ),
        'mysql_db_name': Field(
            String,
            is_required=False,
            description='Database name to connect to for executing queries'
        )
    }
)
def mysql_connection(solid_context: SolidExecutionContext) -> MySQLdb.connection:
    """Instantiate a connection to a MySQL database

    :param solid_context:
    :type solid_context:

    :returns:

    :rtype:
    """
    return MySQLdb.connect(
        host=solid_context['mysql_hostname'],
        user=solid_context['mysql_username'],
        passwd=solid_context['mysql_password'],
        port=solid_context['mysql_port'],
        db=solid_context['mysql_db_name'])


def run_query(query: Query, db_conn: MySQLdb.connection) -> List[Dict]:
    """Execute the passed query against the MySQL database connection and return the row data as a dictionary.

    :param query: PyPika query object that specifies the desired query
    :type query: Query

    :param db_conn: MySQL connection object
    :type db_conn: MySQLdb.connection

    :returns: Query results as a list of dictionaries

    :rtype: List[Dict]
    """
    db_cursor = db_conn.cursor(DictCursor)
    db_cursor.execute(query)
    return db_cursor.fetchall()
