# treat this as a dictionary and reference variables in the appropriate locations
trino_config = {
    "source": {
        "type": "trino",
        "serviceName": "Starburst Galaxy",
        "serviceConnection": {
            "config": {
                "type": "Trino",
                "hostPort": "mitol-ol-data-lake-production.trino.galaxy.starburst.io:443",  # noqa: E501
                "username": "bot_metadata@mitol.galaxy.starburst.io",
                "authType": {
                    "password": "<retrieve this from https://vault-production.odl.mit.edu/ui/vault/secrets/platform-secrets/kv/starburst-galaxy/details?version=4>"
                },
                "catalog": "ol_data_lake_production",
            }
        },
        "sourceConfig": {
            "config": {
                "type": "DatabaseMetadata",
                "markDeletedTables": True,
                "markDeletedStoredProcedures": True,
                "includeTables": True,
                "includeViews": True,
                "includeTags": True,
                "includeOwners": True,
                "includeStoredProcedures": True,
                "includeDDL": True,
                "threads": 10,
            }
        },
    },
    "sink": {"type": "metadata-rest", "config": {}},
    "workflowConfig": {
        "loggerLevel": "INFO",
        "openMetadataServerConfig": {
            "hostPort": "https://open-metadata-qa.ol.mit.edu/api",
            "authProvider": "openmetadata",
            "securityConfig": {
                "jwtToken": "<retrieve this from https://open-metadata-ci.ol.mit.edu/bots/ingestion-bot>"
            },
            "storeServiceConnection": True,
        },
    },
}
