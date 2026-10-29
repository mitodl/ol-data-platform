{% macro duckdb_init() %}
  {# Initialize DuckDB with required extensions for local development #}
  {% if target.type == 'duckdb' %}
    {{ log("DuckDB target detected: " ~ target.name, info=True) }}
    {{ log("Database path: " ~ target.path, info=True) }}

    {# DuckDB 2.0 removed CALL load_aws_credentials(). VALIDATION 'none' lets the
       unit-test target, which reads no S3, start without AWS credentials. #}
    {% set _ = run_query("CREATE OR REPLACE SECRET aws_credential_chain (TYPE S3, PROVIDER credential_chain, VALIDATION 'none')") %}
    {{ log("Created the AWS credential chain secret", info=True) }}
  {% endif %}
{% endmacro %}

{% macro iceberg_source(source_name, table_name) %}
  {#
    Macro to reference Iceberg tables from AWS Glue catalog

    For DuckDB targets: References the view created by register-glue-sources.py
    For Trino targets: Uses normal {{ source() }} macro

    Usage: {{ iceberg_source('ol_warehouse_raw_data', 'raw__mitlearn__app__postgres__users_user') }}
  #}
  {% if target.type == 'duckdb' %}
    {# For DuckDB, reference the Glue-backed view #}
    {# View naming convention: glue__{database}__{table_name} #}
    glue__ol_warehouse_production_raw__{{ table_name }}
  {% else %}
    {# For Trino, use normal source reference #}
    {{ source(source_name, table_name) }}
  {% endif %}
{% endmacro %}
