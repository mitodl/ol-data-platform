{% macro dev_schema_source(source_name, table_name) %}
{#
    Prefers a Dagster source's dev per-schema-suffix table if it exists, else the
    shared/production schema, else neither (caller stubs). Skipped for unit tests.

    Returns: is_unit_test, source_ref, primary_relation, fallback_relation,
    resolved_relation (primary or fallback), relation_ref (what to select FROM).
#}
    {% set source_ref = source(source_name, table_name) %}
    {% set is_unit_test = model.resource_type == 'unit_test' %}
    {% set primary_relation = none %}
    {% set fallback_relation = none %}
    {% if not is_unit_test %}
        {% set primary_relation = adapter.get_relation(
            database=source_ref.database,
            schema=source_ref.schema,
            identifier=source_ref.identifier
        ) %}
        {% set fallback_schema = source_ref.schema.replace(var("schema_suffix", ""), "").rstrip("_") %}
        {% if not primary_relation and fallback_schema != source_ref.schema %}
            {% set fallback_relation = adapter.get_relation(
                database=source_ref.database,
                schema=fallback_schema,
                identifier=source_ref.identifier
            ) %}
        {% endif %}
    {% endif %}
    {{ return({
        "is_unit_test": is_unit_test,
        "source_ref": source_ref,
        "primary_relation": primary_relation,
        "fallback_relation": fallback_relation,
        "resolved_relation": primary_relation or fallback_relation,
        "relation_ref": fallback_relation if (not is_unit_test and not primary_relation and fallback_relation) else source_ref,
    }) }}
{% endmacro %}
