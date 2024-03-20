{% macro deduplicate_data(cte_name1='source', cte_name2='most_recent_source') %}
 --- guard against duplications by airbyte Incremental Sync - Append and the cursor field is a timestamp
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
