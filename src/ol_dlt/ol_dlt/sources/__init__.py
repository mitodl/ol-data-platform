"""dlt sources for the MIT Open Learning data platform.

Each submodule exposes a ``@dlt.source`` (and its ``@dlt.resource`` bodies) for
one upstream system. Sources contain no environment branching and no Dagster
imports; destination/dataset/table_format are resolved by ``ol_dlt.config``.
"""
