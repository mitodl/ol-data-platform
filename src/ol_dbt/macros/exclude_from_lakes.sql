{% macro exclude_from_lakes(lakes) -%}
    {#
        A predicate that empties a model in the lakes named, for a branch a lake
        does not carry. Call it inside the model's source filter:

            where ... {{ exclude_from_lakes(['qa']) }}

        In a named lake it renders `and false`, so the model builds with its full
        schema and no rows, and the unions that read it still build. Elsewhere it
        renders nothing, so the compiled SQL there is unchanged.

        An incremental model only stops adding rows: what its table already
        holds in that lake stays until a --full-refresh of it and its
        incremental children.

        The lake is the one raw_data_lake() reads from the target. Use it for a
        branch the lake leaves out on purpose (RFC 12711: the edxorg tracking
        logs are not a QA branch, and a first build of them exceeds the QA
        StarRocks batch resource group), not for a raw table that is merely
        missing: that belongs in the inventory and the QA branch baseline.
    #}
    {%- if raw_data_lake() in lakes %}
        and false
    {%- endif -%}
{%- endmacro %}
