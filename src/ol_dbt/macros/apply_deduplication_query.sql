{% macro raw_extracted_at(raw_table) %}
    {#
        Resolve the raw-metadata column a staging model orders by to pick the newest
        copy of a record key. Returns a column name, a list of them in precedence
        order, or none when the table has no such column, which means "do not
        deduplicate this table".

        A list is for tables whose models break ties on a second loader column,
        such as `_ab_source_file_last_modified` on Airbyte S3-source tables. Keeping
        that column in the inventory rather than in model SQL is what lets it
        change with the unit's loader.

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


{% macro deduplicate_raw_table(raw_table=none, order_by=none, partition_columns='id', first_by=none, then_by=none, source_cte='source') %}
    {#
        Collapse duplicate raw rows to the most recent copy per record key, emitting a
        `most_recent_<source_cte>` CTE (`most_recent_source` by default) for the model
        to select from.

        Pass `raw_table` and the ordering column is resolved from the inventory. Pass
        `order_by` to override it, which is what the models ordering by a business
        column (modified, updated_on, systemmodstamp, ...) already do -- those are
        loader-agnostic and need no inventory entry.

        `then_by` appends a business-column tie-breaker after the resolved ordering,
        so a model can keep one without naming the loader's columns itself.
        `first_by` prepends one instead, for a model whose primary ordering is a
        business column (updated_on) with the loader column only breaking ties.
        When the resolved column is none, `first_by` does not force a dedup: the
        pass-through below still applies.

        `source_cte` names the CTE to read, for a model that deduplicates more than
        one raw table.

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
    {%- if order_by is not none and first_by is not none -%}
        {{ exceptions.raise_compiler_error(
            "deduplicate_raw_table: first_by prepends to the inventory-resolved ordering; "
            ~ "with an explicit order_by, put the column in order_by instead."
        ) }}
    {%- endif -%}

    {%- set resolved = order_by if order_by is not none else raw_extracted_at(raw_table) -%}

    {%- if resolved is none %}
    , most_recent_{{ source_cte }} as (
        select * from {{ source_cte }}
    )
    {%- else %}
    {%- set ordering = ([first_by] if first_by is not none else []) + ([resolved] if resolved is string else resolved) + ([then_by] if then_by is not none else []) %}
    , {{ source_cte }}_sorted as (
        select
            *
            {#-
                NULLS LAST is explicit rather than inherited. Trino documents
                NULLS LAST as its default in both directions, but this ordering
                has to survive the StarRocks migration, and the case it protects
                is real: a unit that starts stamping `_dlt_load_id` leaves rows
                loaded before the flag with a null one. Nulls first would let a
                stale row win the dedup.
            -#}
            , row_number() over (
                partition by {{ partition_columns }}
                order by {% for column in ordering %}{{ column }} desc nulls last{% if not loop.last %}, {% endif %}{% endfor %}
            ) as row_num
        from {{ source_cte }}
    )
    , most_recent_{{ source_cte }} as (
        select * from {{ source_cte }}_sorted
        where row_num = 1
    )
    {%- endif %}
{% endmacro %}
