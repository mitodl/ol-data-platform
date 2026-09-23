{#
    source() override for DuckDB targets: route to the `glue__`-prefixed views registered by
    `ol-dbt local register`. Trino uses the built-in.

    This used to dispatch on the SOURCE NAME through a hand-maintained map, defaulting
    unknown names to the raw database. Two of the three source names actually in use were
    missing from that map: `dimensional` (9 sources) and `reporting` (2), so all 11 resolved
    to `glue__ol_warehouse_production_raw__<table>` -- a view that does not exist, because
    those tables live in the dimensional and reporting Glue databases. A default that points
    somewhere plausible is worse than no default; it turns a missing entry into a confusing
    Catalog Error instead of a clear one.

    The source already declares where it lives. dbt resolves its schema either to a
    fully-qualified Glue database (`ol_warehouse_production_dimensional`, as the dimensional
    and reporting sources declare) or to `<target.schema>_<layer>` (`main_raw`), so both
    conventions are derivable and neither needs a list kept in sync.
#}
{% macro source(source_name, table_name) %}
  {% if target.type == 'duckdb' %}
    {#- Registers the source.* dependency edge in the manifest so lineage and
        state:modified+ selection still see it on DuckDB targets. The returned relation is
        deliberately discarded; the Glue view below is what the SQL reads. There is a CI
        guard for this specific edge (.github/workflows/dbt_pr_ci.yaml). -#}
    {% set builtin_relation = builtins.source(source_name, table_name) %}

    {#- Two contexts hand back a relation there is nothing to derive from, and in both the
        value is never rendered as a table name:
          - parsing, where the schema is still target.schema; only the dependency
            registration above matters.
          - a unit test, where dbt substitutes a fixture CTE for this input and the
            relation arrives with schema None. -#}
    {% if not execute or builtin_relation is none or builtin_relation.schema is none %}
      {{ return(builtin_relation) }}
    {% endif %}

    {% set prefix = target.schema ~ '_' %}
    {% if builtin_relation.schema.startswith('ol_warehouse_') %}
      {% set glue_database = builtin_relation.schema %}
    {% elif builtin_relation.schema.startswith(prefix) %}
      {% set glue_database = 'ol_warehouse_production_'
                             ~ builtin_relation.schema[prefix | length:] %}
    {% else %}
      {% do exceptions.raise_compiler_error(
        "No Glue database can be derived for source('" ~ source_name ~ "', '" ~ table_name
        ~ "'): its schema is '" ~ builtin_relation.schema ~ "', which is neither a "
        ~ "fully-qualified ol_warehouse_* database nor '" ~ prefix ~ "<layer>'."
      ) %}
    {% endif %}

    {{ return(api.Relation.create(
      database=target.database,
      schema=target.schema,
      identifier='glue__' ~ glue_database ~ '__' ~ table_name
    )) }}
  {% else %}
    {{ return(builtins.source(source_name, table_name)) }}
  {% endif %}
{% endmacro %}
