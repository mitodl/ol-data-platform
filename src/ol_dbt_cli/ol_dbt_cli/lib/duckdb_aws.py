"""DuckDB statements for reaching S3 and Glue with the local AWS credentials."""

# DuckDB 2.0's aws extension removed `CALL load_aws_credentials()`. A
# credential_chain secret resolves the same AWS SDK chain (env, profile, SSO)
# and works on 1.x too. Without a VALIDATION clause, creating it fails when the
# chain finds no credentials, rather than at the first S3 read.
USE_AWS_CREDENTIAL_CHAIN_SQL = "CREATE OR REPLACE SECRET aws_credential_chain (TYPE S3, PROVIDER credential_chain)"
