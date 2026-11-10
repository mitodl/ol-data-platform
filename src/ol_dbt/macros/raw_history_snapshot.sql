{#-
    Body of a snapshot that keeps the history of one raw table: one row per
    version of each record, and a `dbt_is_deleted` row when the record leaves
    the source.

    The snapshot files calling this are generated from the ingestion inventory
    by `ol-dbt inventory history --write` (INGESTION_INVENTORY_SPEC.md §11), so
    the arguments come from the table's unit file, not from hand edits.

    Columns left out:
    - the loader's own (`_airbyte_*`, `_ab_*`, `_dlt_*`). They change on every
      re-read of an unchanged row, so they would write a new version of every
      row on every run, and they differ between the Airbyte and dlt loads of
      the same table.
    - any whose name contains a secret pattern (password, secret, token,
      api_key and the like, RAW_HISTORY_SECRET_PATTERNS below), because a
      history table keeps every value it ever saw.
    - the table's `exclude_columns`.

    Duplicate raw rows of one key are collapsed to the newest, ordered as
    deduplicate_raw_table orders them for staging, and the number collapsed is
    logged. A table whose inventory entry names no ordering column cannot pick
    one, so duplicates there fail the run.

    Deletes: with `absence='deleted'` (hard_deletes: new_record) a record
    missing from the select gets a dbt_is_deleted 'True' row. That happens for
    a table whose load reads every row, for a keyset table once its keyset is
    read, and for a dlt merge feed through the newest-load filter
    (raw_delete_filter). With `absence='aged_out'` (hard_deletes: ignore) a
    missing record is left as it was, and the table has no dbt_is_deleted.

    A load that leaves raw empty or short would otherwise make the snapshot
    write a delete row for every missing key, which cannot be undone, so the
    run first counts the current keys missing from the select
    (raw_history_delete_guard) and fails, writing nothing, when too many are
    missing (see that macro for the rule and its vars,
    raw_history_max_delete_fraction, raw_history_min_delete_count and
    raw_history_allow_deletes).

    Args:
        raw_table: name of the table in the `ol_warehouse_raw_data` source.
        unique_key: the source table's primary key column(s).
        absence: 'deleted' or 'aged_out', the unit's history.absence.
        exclude_columns: columns not stored, on top of the defaults above.
        ignore_columns: columns stored but not compared, so a change to them
            alone writes no new version (strategy raw_history_check).
        live: false generates the snapshot disabled, for a unit not rolled out.
        max_delete_fraction: the largest share of current keys one run may
            delete. Defaults to var('raw_history_max_delete_fraction', 0.1).
-#}
{% macro raw_history_snapshot(
    raw_table,
    unique_key,
    absence='deleted',
    exclude_columns=[],
    ignore_columns=[],
    live=true,
    max_delete_fraction=none
) %}
    {%- set hard_deletes = {'deleted': 'new_record', 'aged_out': 'ignore'}.get(absence) -%}
    {%- if hard_deletes is none -%}
        {{ exceptions.raise_compiler_error(
            "raw_history_snapshot: absence must be 'deleted' or 'aged_out', got " ~ absence
        ) }}
    {%- endif -%}
    {%- set key_names = [unique_key] if unique_key is string else unique_key -%}
    {%- set keys = [] -%}
    {%- for key in key_names -%}
        {%- do keys.append(key if modules.re.fullmatch('[a-z_][a-z0-9_]*', key) else adapter.quote(key)) -%}
    {%- endfor -%}
    {%- if ignore_columns -%}
        {{ config(
            enabled=live,
            unique_key=keys[0] if keys | length == 1 else keys,
            strategy='raw_history_check',
            hard_deletes=hard_deletes,
            meta={'raw_history_ignore_columns': ignore_columns}
        ) }}
    {%- else -%}
        {{ config(
            enabled=live,
            unique_key=keys[0] if keys | length == 1 else keys,
            strategy='check',
            check_cols='all',
            hard_deletes=hard_deletes
        ) }}
    {%- endif -%}
    {%- set relation = source('ol_warehouse_raw_data', raw_table) -%}
    {%- set loader_prefixes = ['_airbyte_', '_ab_', '_dlt_'] -%}
    {%- set excluded_names = exclude_columns | map('lower') | list -%}
    {%- set kept = [] -%}
    {%- set dropped = [] -%}
    {%- if execute -%}
        {%- for column in adapter.get_columns_in_relation(relation) -%}
            {%- set name = column.name | lower -%}
            {%- set ns = namespace(loader_column=false, secret=false) -%}
            {%- for prefix in loader_prefixes -%}
                {%- if name.startswith(prefix) -%}
                    {%- set ns.loader_column = true -%}
                {%- endif -%}
            {%- endfor -%}
            {%- for pattern in raw_history_secret_patterns() -%}
                {%- if pattern in name -%}
                    {%- set ns.secret = true -%}
                {%- endif -%}
            {%- endfor -%}
            {%- if ns.secret or name in excluded_names -%}
                {%- do dropped.append(name) -%}
            {%- elif not ns.loader_column -%}
                {%- do kept.append(adapter.quote(column.name)) -%}
            {%- endif -%}
        {%- endfor -%}
        {#- `select *` would bring the loader's columns in and version every
            row on every load, so an empty column list must stop a run that
            writes. `compile` and `docs generate` also execute this, on targets
            that hold no raw tables (the docs workflow and the image build use
            DuckDB), so they keep the placeholder. -#}
        {%- if not kept and flags.WHICH in ['snapshot', 'build'] -%}
            {{ exceptions.raise_compiler_error(
                "raw_history_snapshot: no columns found for " ~ relation
            ) }}
        {%- endif -%}
        {%- for key in key_names if (key | lower) in dropped -%}
            {{ exceptions.raise_compiler_error(
                "raw_history_snapshot: " ~ raw_table ~ " key column " ~ key
                ~ " matches a secret pattern, so it would be left out"
            ) }}
        {%- endfor -%}
    {%- endif -%}
    {%- set delete_filter = raw_delete_filter(raw_table, relation) -%}
    {%- set filtered -%}
        select * from {{ relation }}
        {%- if delete_filter %}
        where {{ delete_filter }}
        {%- endif %}
    {%- endset -%}
    {%- set body -%}
    with raw_rows as (
        {{ filtered }}
    )
    {{ deduplicate_raw_table(raw_table=raw_table, partition_columns=keys | join(', '), source_cte='raw_rows') }}
    select {{ kept | join(', ') if kept else '*' }}
    from most_recent_raw_rows
    {%- endset -%}
    {%- if execute and flags.WHICH in ['snapshot', 'build'] -%}
        {%- if dropped -%}
            {{ log("raw_history_snapshot " ~ this.identifier ~ ": not storing " ~ dropped | join(', '), info=true) }}
        {%- endif -%}
        {%- do raw_history_log_duplicates(filtered, keys, raw_extracted_at(raw_table) is none) -%}
        {%- if absence == 'deleted' -%}
            {%- do raw_history_delete_guard(body, keys, max_delete_fraction) -%}
        {%- endif -%}
    {%- endif %}
    {{ body }}
{% endmacro %}


{#-
    Substrings that mark a column as a secret the raw history does not store.
    Matched against the lower-cased column name, so `access_token`,
    `secret_key` and `password` all match.
-#}
{% macro raw_history_secret_patterns() %}
    {{ return(['password', 'passwd', 'secret', 'token', 'api_key', 'apikey', 'private_key', 'access_key', 'credential']) }}
{% endmacro %}


{#-
    Log how many raw rows the snapshot collapses because they repeat a key,
    and fail when the table has duplicates but nothing to order them by.
-#}
{% macro raw_history_log_duplicates(filtered, keys, unordered) %}
    {%- set count_sql -%}
        select coalesce(sum(copies - 1), 0)
        from (
            select count(*) as copies
            from ({{ filtered }}) as raw_rows
            group by {{ keys | join(', ') }}
            having count(*) > 1
        ) as repeated
    {%- endset -%}
    {%- set collapsed = (run_query(count_sql).rows[0][0] or 0) | int -%}
    {{ log(
        "raw_history_snapshot " ~ this.identifier ~ ": collapsed " ~ collapsed
        ~ " duplicate raw row(s)",
        info=true
    ) }}
    {%- if collapsed and unordered -%}
        {{ exceptions.raise_compiler_error(
            "raw_history_snapshot: " ~ this.identifier ~ " reads " ~ collapsed
            ~ " duplicate raw row(s), and its inventory entry declares no "
            ~ "raw_metadata_column to pick the newest by. Nothing was written."
        ) }}
    {%- endif -%}
{% endmacro %}


{#-
    dbt's `check` strategy with every selected column compared except the
    snapshot's `meta.raw_history_ignore_columns`. Ignored columns are still
    stored; a change to them alone writes no new version. The column list is
    read from the query at run time, because `check_cols` is fixed at parse
    time, before the raw table's columns are known, and a column the source
    adds later must be compared.
-#}
{% macro snapshot_raw_history_check_strategy(node, snapshotted_rel, current_rel, model_config, target_exists) %}
    {%- set ignored = config.get('meta')['raw_history_ignore_columns'] | map('lower') | list -%}
    {%- set compared = [] -%}
    {%- for column in get_columns_in_query(node['compiled_code']) if (column | lower) not in ignored -%}
        {%- do compared.append(adapter.quote(column)) -%}
    {%- endfor -%}
    {%- set primary_key = config.get('unique_key') -%}
    {%- set hard_deletes = adapter.get_hard_deletes_behavior(config) -%}
    {%- set updated_at = config.get('updated_at') or snapshot_get_time() -%}
    {%- set column_added, check_cols = snapshot_check_all_get_existing_columns(node, target_exists, compared) -%}
    {%- set row_changed_expr -%}
    (
    {%- if column_added -%}
        {{ get_true_sql() }}
    {%- else -%}
    {%- for col in check_cols -%}
        {{ snapshotted_rel }}.{{ col }} != {{ current_rel }}.{{ col }}
        or
        (
            (({{ snapshotted_rel }}.{{ col }} is null) and not ({{ current_rel }}.{{ col }} is null))
            or
            ((not {{ snapshotted_rel }}.{{ col }} is null) and ({{ current_rel }}.{{ col }} is null))
        )
        {%- if not loop.last %} or {% endif -%}
    {%- endfor -%}
    {%- endif -%}
    )
    {%- endset %}
    {%- set scd_args = api.Relation.scd_args(primary_key, updated_at) -%}
    {%- do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": row_changed_expr,
        "scd_id": snapshot_hash_arguments(scd_args),
        "invalidate_hard_deletes": hard_deletes == 'invalidate',
        "hard_deletes": hard_deletes
    }) -%}
{% endmacro %}


{#-
    Fail a raw history snapshot run that would mark too many records deleted.

    Counts the snapshot's current keys (not already marked deleted) that are
    missing from `body`, the select the snapshot reads. Raises, so the run
    writes nothing, when the missing keys are more than `max_delete_fraction`
    of the current keys, unless they are also no more than
    var('raw_history_min_delete_count', 10) and fewer than half of them. The
    floor lets a small table lose a few records without failing every run
    until someone overrides it; the half keeps a small table whose load came
    up short (or empty) from passing under it. The first run of a snapshot
    has no current keys and is not checked.

    A per-table `max_delete_fraction` takes precedence over the var. A
    deliberate mass delete (a loader cutover, or a table's first keyset run)
    passes when the run lists the snapshot in
    var('raw_history_allow_deletes'), e.g.
    --vars '{raw_history_allow_deletes: [snapshot_raw__x]}'. The count is
    logged either way, to compare with the expected key diff.
-#}
{% macro raw_history_delete_guard(body, unique_key, max_delete_fraction=none) %}
    {%- set target_relation = adapter.get_relation(
        database=this.database, schema=this.schema, identifier=this.identifier
    ) -%}
    {%- if target_relation is none -%}
        {{ return(none) }}
    {%- endif -%}
    {%- set keys = [unique_key] if unique_key is string else unique_key -%}
    {%- set limit = max_delete_fraction
        if max_delete_fraction is not none
        else var('raw_history_max_delete_fraction', 0.1) -%}
    {%- set floor = var('raw_history_min_delete_count', 10) -%}
    {%- set allow_list = var('raw_history_allow_deletes', []) -%}
    {%- if limit is not number or floor is not number -%}
        {{ exceptions.raise_compiler_error(
            "raw_history_delete_guard: the delete fraction and "
            ~ "raw_history_min_delete_count must be numbers, got "
            ~ limit ~ " and " ~ floor
        ) }}
    {%- endif -%}
    {#- A mapping is iterable, and `in` would test only its keys, so
        {snapshot_x: false} would still allow the deletes. -#}
    {%- if allow_list is string or allow_list is mapping or allow_list is not iterable -%}
        {{ exceptions.raise_compiler_error(
            "raw_history_allow_deletes must be a list of snapshot names, got "
            ~ allow_list
        ) }}
    {%- endif -%}
    {%- set guard_sql -%}
        with source_keys as (
            select distinct {{ keys | join(', ') }}
            from ({{ body }}) as source_rows
        )

        , current_keys as (
            select distinct {{ keys | join(', ') }}
            from {{ target_relation }}
            where dbt_valid_to is null and coalesce(dbt_is_deleted, 'False') != 'True'
        )

        select
            count(*) as current_count
            , sum(case when source_keys.{{ keys[0] }} is null then 1 else 0 end)
                as missing_count
        from current_keys
        left join source_keys
            on
            {%- for key in keys %}
                {% if not loop.first %}and {% endif -%}
                current_keys.{{ key }} = source_keys.{{ key }}
            {%- endfor %}
    {%- endset -%}
    {%- set row = run_query(guard_sql).rows[0] -%}
    {%- set current_count = row[0] | int -%}
    {%- set missing_count = (row[1] or 0) | int -%}
    {%- set allowed = this.identifier in allow_list -%}
    {{ log(
        "raw_history_delete_guard " ~ this.identifier ~ ": " ~ missing_count
        ~ " of " ~ current_count ~ " current keys missing from the source"
        ~ (" (allowed for this run)" if allowed else ""),
        info=true
    ) }}
    {%- set everything = current_count > 0 and missing_count == current_count -%}
    {%- set too_many = missing_count > current_count * limit and (
        missing_count > floor or missing_count * 2 >= current_count
    ) -%}
    {%- if not allowed and (everything or too_many) -%}
        {{ exceptions.raise_compiler_error(
            "raw_history_delete_guard: " ~ this.identifier ~ " would mark "
            ~ missing_count ~ " of its " ~ current_count ~ " current records "
            ~ "deleted, more than " ~ limit ~ " of them (up to " ~ floor
            ~ " pass while fewer than half). Nothing was written. "
            ~ "If the raw table is complete, rerun with --vars "
            ~ "'{raw_history_allow_deletes: [" ~ this.identifier ~ "]}'."
        ) }}
    {%- endif -%}
{% endmacro %}
