from dagster_sling import SlingConnectionResource, SlingResource


def create_sling_resource(source_bucket_name, glue_warehouse):
    """Create SlingResource instance with the appropriate connections.

    Args:
        source_bucket_name (str): The name of the S3 source bucket.
          e.g., "ol-data-lake-landing-zone-qa".
        glue_warehouse (str): The Destination Glue warehouse location.
          e.g., "s3://ol-data-lake-raw-qa/".

    Returns:
        SlingResource: Configured SlingResource instance.
    """
    return SlingResource(
        connections=[
            SlingConnectionResource(
                name="S3_SOURCE",
                type="s3",
                bucket=source_bucket_name,
                s3_region="us-east-1",
            ),
            SlingConnectionResource(
                name="ICEBERG",
                type="iceberg",
                catalog_type="glue",
                glue_warehouse=glue_warehouse,
                s3_region="us-east-1",
            ),
        ]
    )


def edxorg_replication_config(source_bucket_prefix, glue_namespace):
    """Generate the Sling replication configuration for edxorg data streams.

    Args:
        source_bucket_name (str): The name of the S3 source bucket.
          e.g., "ol-data-lake-landing-zone-qa".
        glue_warehouse (str): The Destination Glue warehouse location.
          e.g., "s3://ol-data-lake-raw-qa/".

    Returns:
        dict: Replication configuration dictionary for edxorg streams.
    """
    return {
        "source": "S3_SOURCE",
        "target": "ICEBERG",
        "defaults": {
            "mode": "incremental",
            "source_options": {"delimiter": "\t"},
            # uses the _sling_loaded_at column in the target table
            "update_key": "_sling_loaded_at",
            "debug": True,
        },
        "streams": {
            f"{source_bucket_prefix}/auth_user/prod/": {
                "object": f"{glue_namespace}.raw__edxorg__s3__auth_user",
            },
            f"{source_bucket_prefix}/student_courseenrollment/prod/": {
                "object": f"{glue_namespace}.raw__edxorg__s3__student_courseenrollment",
                "source_options": {
                    "delimiter": "\t",
                    "chunk_size": 10000,
                },
            },
        },
        "env": {
            "SLING_LOADED_AT_COLUMN": "timestamp",
        },
    }
