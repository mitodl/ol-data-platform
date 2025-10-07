"""
Open Learning Library is an installation of Open edX that hosts courses that are no
longer active on other systems (e.g. edx.org or MITx Online).  We need to be able to
maintain a copy of these courses for use in the search functionality of MIT Open.  To
that end, this keeps the MIT GitHub repositories that store the course contents
synchronized with S3 for ingestion by the MIT Open application.
"""

from dagster import sensor, asset, DynamicPartitionsDefinition  # noqa: I001, F401


@sensor()
def oll_github_repos(): ...
