# -*- coding: utf-8 -*-
from pathlib import PosixPath

from dagster import usable_as_dagster_type


@usable_as_dagster_type
class DagsterPath(PosixPath):
    pass
