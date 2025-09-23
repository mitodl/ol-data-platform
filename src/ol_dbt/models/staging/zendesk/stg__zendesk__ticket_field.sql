with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__thirdparty__zendesk_support__ticket_fields") }})

    {{ deduplicate_raw_table(order_by="_airbyte_extracted_at", partition_columns="id") }},
    cleaned as (
        select
            id as field_id,
            url as field_api_url,
            description as field_description,
            title as field_title,
            active as field_is_active,
            {{ cast_timestamp_to_iso8601("created_at") }} as field_created_at,
            {{ cast_timestamp_to_iso8601("updated_at") }} as field_updated_at
        from most_recent_source
    )

select *
from cleaned
