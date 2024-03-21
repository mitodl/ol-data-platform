{% macro deduplicate_query(cte_name1='source', cte_name2='most_recent_source') %}
    --- guard against duplication introduced by airbyte sync mode "Incremental Sync - Append"
    --- where cursor is set to a timestamp field.
    --- The deduplication works like this - if there are multiple copies of the same record based on primary key - id,
    --- we will use the record from most recent sync in the source table. If there is only one record for the same
    --- primary key, it will just load that record as it is.
   , source_sorted as (
        select
            *
            , row_number() over ( partition by id order by _airbyte_emitted_at desc) as row_num
        from {{ cte_name1 }}
    )

    , {{ cte_name2 }} as (
        select *
        from source_sorted
        where row_num = 1
    )

{% endmacro %}
