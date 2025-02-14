{% macro deduplicate_query(cte_name1='source', cte_name2='most_recent_source', partition_columns='id') %}
    /*
        Add additional queries to handle duplicated data introduced by airbyte sync mode "Incremental Sync - Append"
        where cursor is set to a timestamp field. The deduplication logic works like this:
        - If there are multiple copies of the same record based on partition_columns, we will use the record from most
          recent sync in the source table.
        - If there is only one record for the same partition_columns, it will just load that record as it is.
    */
   , source_sorted as (
        select
            *
            , row_number() over ( partition by {{ partition_columns }} order by _airbyte_emitted_at desc) as row_num
        from {{ cte_name1 }}
    )

    , {{ cte_name2 }} as (
        select *
        from source_sorted
        where row_num = 1
    )

{% endmacro %}

{% macro deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='id') %}
    /*
        This dedupe applied to the raw tables via Incremental Sync from Airbyte. It will deduplicate the data based on
        the partition_columns and order by columns.
    */
   , source_sorted as (
        select
            *
            , row_number() over ( partition by {{ partition_columns }} order by {{ order_by }} desc) as row_num
        from source
    )

    , most_recent_source as (
        select * from source_sorted
        where row_num = 1
    )

{% endmacro %}
