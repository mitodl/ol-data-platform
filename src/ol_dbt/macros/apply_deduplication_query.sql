{% macro raw_extracted_at(raw_table) %}
    {#
        Resolve the raw-metadata column a staging model orders by to pick the newest
        copy of a record key. Returns none when the table has no such column, which
        means "do not deduplicate this table".

        Resolved per RAW TABLE through the ingestion inventory, not per dbt source and
        not from `source.loader`. One dbt source mixes tables from units with different
        loaders -- edxorg's raw namespace alone nests three -- and `loader:` in the
        sources YAML is unreliable: `_edxorg_sources.yml` declares `loader: airbyte`
        over 18 dlt-produced tables (INGESTION_INVENTORY_SPEC.md §1.2).

        The map is generated from ingestion/inventory/ by
        `ol-dbt inventory metadata-columns --write`.
    #}
    {%- set mapping = raw_metadata_column_map() -%}
    {%- if raw_table not in mapping -%}
        {#-
            An undeclared table is a missing inventory entry, not a table without a
            metadata column. Guessing either way is what this macro exists to stop:
            defaulting to the Airbyte column is how dlt-loaded tables came to be
            deduplicated on a column they never had.
        -#}
        {{ exceptions.raise_compiler_error(
            "raw_extracted_at: '" ~ raw_table ~ "' is not declared in the ingestion inventory. "
            ~ "Add it to its (deployment, layer) unit under ingestion/inventory/units/, then run "
            ~ "`ol-dbt inventory metadata-columns --write`."
        ) }}
    {%- endif -%}
    {%- do return(mapping[raw_table]) -%}
{% endmacro %}


{% macro deduplicate_raw_table(raw_table=none, order_by=none, partition_columns='id') %}
    {#
        Collapse duplicate raw rows to the most recent copy per record key, emitting a
        `most_recent_source` CTE for the model to select from.

        Pass `raw_table` and the ordering column is resolved from the inventory. Pass
        `order_by` to override it, which is what the models ordering by a business
        column (modified, updated_on, systemmodstamp, ...) already do -- those are
        loader-agnostic and need no inventory entry.

        Where the resolved column is none, this emits a PASS-THROUGH: the raw table
        carries no metadata column, so there is nothing to order by and nothing to
        deduplicate. That is the correct result for dlt-loaded units rather than a
        degraded one -- dlt's `merge` disposition dedups on the primary key within a
        load, and `replace` leaves one row per key by construction. The dedup step
        exists for Airbyte's "Incremental Sync - Append" mode.

        There is deliberately no default ordering column. The old default was
        `_airbyte_extracted_at`, which is how a model could acquire a dependency on
        Airbyte's metadata without anyone writing it down -- and keep it silently after
        the source moved to dlt.
    #}
    {%- if order_by is none and raw_table is none -%}
        {{ exceptions.raise_compiler_error(
            "deduplicate_raw_table requires either raw_table (resolve the ordering column "
            ~ "from the inventory) or an explicit order_by (a business column)."
        ) }}
    {%- endif -%}

    {%- set resolved = order_by if order_by is not none else raw_extracted_at(raw_table) -%}

    {%- if resolved is none %}
    , most_recent_source as (
        select * from source
    )
    {%- else %}
    , source_sorted as (
        select
            *
            , row_number() over ( partition by {{ partition_columns }} order by {{ resolved }} desc) as row_num
        from source
    )
    , most_recent_source as (
        select * from source_sorted
        where row_num = 1
    )
    {%- endif %}
{% endmacro %}
