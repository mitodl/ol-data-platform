{#
    dbt's built-in generate_schema_name *appends* a model's custom `+schema` to
    target.schema. The b2b_analytics models set `+schema: b2b_analytics`
    (dbt_project.yml) so DbtAutomationTranslator.get_group_name -- which reads
    config.schema, not the profile-resolved schema -- groups them, and the
    starrocks profiles already use `schema: b2b_analytics`. The default macro
    turns that pair into `b2b_analytics_b2b_analytics`.

    Nothing else in the stack expects that name. The substructure stack creates
    and grants exactly one database (`CREATE DATABASE IF NOT EXISTS
    b2b_analytics` + CREATE TABLE / CREATE MATERIALIZED VIEW to role `app`, in
    ol-infrastructure substructure/starrocks/__main__.py), Dagster's
    StarRocksResource connects with database="b2b_analytics", and
    ol-analytics-api queries `settings.starrocks_schema`, default
    "b2b_analytics". Only dbt disagreed -- and it only got away with creating
    the extra database because the Dagster Vault role is `admin`, which can
    CREATE DATABASE. The MVs landed somewhere ungranted, so the `app` role that
    the API authenticates as could not have read them anyway.

    For StarRocks, therefore, treat `+schema` as the literal schema name.

    Every other target keeps dbt's default concatenation: this macro file is
    shared by both dbt projects in this repo (the Trino-scoped one in dbt.py and
    the StarRocks-scoped one in dbt_starrocks.py), and the Trino/Snowflake
    models already depend on `<target.schema>_<custom>` naming for every mart.
    Guarding on target.type keeps this fix from silently relocating them.

    On Trino the schema is a Glue database, and Glue accepts names that Iceberg's
    GlueCatalog refuses to load (IcebergToGlueConverter.GLUE_DB_PATTERN,
    `[a-z0-9_]{1,252}`, enforced unless the catalog sets
    glue.skip-name-validation, which ours do not). Trino creates such a database
    without complaint, and then nothing that reads the lake through Iceberg
    (Gravitino, StarRocks) can open it. Both cases found in the lake came from
    `schema_suffix`: the literal `<your name>` placeholder and a hyphenated
    branch name. Fail the run before the database exists.

    Uppercase is refused as well. Trino would most likely fold it to lower case,
    which makes the database loadable but not the one the suffix names. An empty
    `schema_suffix` (e.g. a blank DBT_SCHEMA_SUFFIX in Dagster dev) renders as
    `None` and is refused for the same reason.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.type == 'starrocks' and custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {%- set schema_name = default__generate_schema_name(custom_schema_name, node) | trim -%}
        {%- if target.type == 'trino' and not modules.re.fullmatch('[a-z0-9_]{1,252}', schema_name) -%}
            {{ exceptions.raise_compiler_error(
                "Schema name '" ~ schema_name ~ "' is not one Iceberg can load from Glue. "
                ~ "It must be 1-252 characters of lowercase letters, digits and underscores. "
                ~ "Check the schema_suffix var (currently '" ~ var('schema_suffix', '') ~ "'; "
                ~ "'None' means it is empty, e.g. a blank DBT_SCHEMA_SUFFIX)."
            ) }}
        {%- endif -%}
        {{ schema_name }}
    {%- endif -%}
{%- endmacro %}
