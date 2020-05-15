# -*- coding: utf-8 -*-

from typing import Text

from pathlib import Path

from dagster import Int
from pymongo import MongoClient


class MongoDBClient:

    def __init__(self, hostname: Text, username: Text, password: Text, port: Int=27017):
        """Create a connection to a MongoDB instance for use within solid execution logic

        :param hostname: Host string for connecting to the MongoDB instance
        :type hostname: Text

        :param username: Username for authenticating to the Mongo database
        :type username: Text

        :param password: Password for authenticating to the Mongo database
        :type password: Text

        :param port: TCP port that the server is listening on
        :type port: Int
        """
        self.client = MongoClient(host=hostname, port=port, username=username, password=password)

    def export_db(self, db_name: Text) -> Path:
        """Run a mongo dump for the specified database and return the path to the exported data.

        :param db_name: Name of the database to export
        :type db_name: Text

        :returns: Path to the exported database contents

        :rtype: Path
        """
        self.client.get_database(db_name)
