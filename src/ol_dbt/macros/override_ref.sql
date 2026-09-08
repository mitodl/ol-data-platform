{#
    ref() override for the local DuckDB dev target.

    Build only what you changed: a model that exists locally is used, and one that does not
    falls back to the production Glue view registered by `ol-dbt local register`. Everything
    upstream of your change is then read from production instead of rebuilt.

    Two things this deliberately does NOT do, both of which it used to:

    1. Dispatch on the model NAME. The old version matched `stg__`, `int__`, `dim_`,
       `marts__`, `rpt__` and sent everything else to a branch that returned the local
       relation with the comment "will fail if doesn't exist" -- which is what happened.
       Measured against the manifest, 165 of 672 models matched no prefix: all 110 external,
       all 27 reporting (the `rpt__` branch matched nothing -- reporting models are bare, e.g.
       enrollment_detail_report), 11 integrations, the 11 `__`-prefixed models under
       intermediate/*/subqueries/, 5 migration and 1 dimensional. A quarter of the project
       was silently exempt from the fallback this macro exists to provide.

       The node already knows its layer. dbt resolves a model's schema to
       `<target.schema>_<config.schema>` (main_intermediate, main_staging, ...), and that
       layer maps 1:1 onto the Glue database, so deriving it covers every model with no
       special cases and no list to keep in sync.

    2. Return a bare string. `{{ glue_view_name }}` rendered as text rather than a Relation,
       which is why dbt unit tests cannot run on this target at all -- the unit-test
       machinery calls Relation methods on whatever ref() returns and dies with
       "'NoneType' object has no attribute 'lower'".

    An unmappable model now raises instead of returning a relation that cannot exist: the
    failure names the ref, rather than surfacing three layers down as a Catalog Error about
    a table nobody wrote.
#}
{% macro ref(model_name) %}
  {% if target.type == 'duckdb' and target.name == 'dev_local' %}
    {% set local_relation = builtins.ref(model_name) %}

    {#- Two contexts hand back a relation there is nothing to derive from, and in both the
        value is never rendered as a table name:
          - parsing, where the schema is still target.schema and there is no adapter to
            probe; only the dependency registration above matters.
          - a unit test, where dbt substitutes a fixture CTE for this input and the
            relation arrives with schema None. -#}
    {% if not execute or local_relation is none or local_relation.schema is none %}
      {{ return(local_relation) }}
    {% endif %}

    {% set relation_exists = adapter.get_relation(
      database=target.database,
      schema=local_relation.schema,
      identifier=local_relation.identifier
    ) %}

    {% if relation_exists %}
      {{ log("Using local table for ref('" ~ model_name ~ "'): " ~ local_relation, info=False) }}
      {{ return(local_relation) }}
    {% endif %}

    {#- `main_intermediate` -> `intermediate`. A model with no custom schema resolves to
        target.schema itself and yields an empty layer, which fails below. -#}
    {% set prefix = target.schema ~ '_' %}
    {% set layer = local_relation.schema[prefix | length:]
                   if local_relation.schema.startswith(prefix) else '' %}

    {% if not layer %}
      {% do exceptions.raise_compiler_error(
        "No Glue database can be derived for ref('" ~ model_name ~ "'): its schema is '"
        ~ local_relation.schema ~ "', which is not '" ~ prefix ~ "<layer>'. Build the model "
        ~ "locally, or give it a +schema so its layer can be resolved."
      ) %}
    {% endif %}

    {% set glue_view_name = 'glue__ol_warehouse_production_' ~ layer ~ '__' ~ model_name %}
    {{ log("Falling back to Glue view for ref('" ~ model_name ~ "'): " ~ glue_view_name, info=True) }}
    {{ return(api.Relation.create(
      database=target.database, schema=target.schema, identifier=glue_view_name
    )) }}
  {% else %}
    {{ return(builtins.ref(model_name)) }}
  {% endif %}
{% endmacro %}
