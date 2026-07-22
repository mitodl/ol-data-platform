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
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.type == 'starrocks' and custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default__generate_schema_name(custom_schema_name, node) }}
    {%- endif -%}
{%- endmacro %}
