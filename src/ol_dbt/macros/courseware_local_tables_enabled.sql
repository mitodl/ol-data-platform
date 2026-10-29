{#
    Whether to enable a model or test that reads courseware tables only the local
    runner (ml.lib.courseware.local) writes. It writes them for the DuckDB lake, so
    they exist on DuckDB targets only, and anywhere else a build fails on a table
    that has never been created. in_warehouse_var names the dbt_project.yml var that
    is set true once a Dagster asset writes them to the warehouse.

    The sources in _courseware__sources.yml repeat the expression, because dbt does
    not make project macros available when it renders yml configs.
#}
{% macro courseware_local_tables_enabled(in_warehouse_var) %}
    {{ return(var(in_warehouse_var) or target.type == 'duckdb') }}
{% endmacro %}
