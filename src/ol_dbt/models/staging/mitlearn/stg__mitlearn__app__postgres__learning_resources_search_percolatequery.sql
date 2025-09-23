with
    source as (
        select *
        from
            {{
                source(
                    "ol_warehouse_raw_data", "raw__mitlearn__app__postgres__learning_resources_search_percolatequery"
                )
            }}
    ),
    cleaned as (
        select
            id as percolatequery_id,
            query as percolatequery_query,
            created_on as percolatequery_created_on,
            updated_on as percolatequery_updated_on,
            source_type as percolatequery_source_type,
            display_label as percolatequery_display_label,
            original_query as percolatequery_original_query
        from source
    )

select *
from cleaned
