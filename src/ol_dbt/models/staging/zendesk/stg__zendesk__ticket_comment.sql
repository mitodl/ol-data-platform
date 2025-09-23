with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__thirdparty__zendesk_support__ticket_comments") }})

    {{ deduplicate_raw_table(order_by="_airbyte_extracted_at", partition_columns="id") }},
    cleaned as (
        select
            id as comment_id,
            ticket_id,
            via as comment_via_object,
            type as comment_type,
            public as comment_is_public,
            uploads as comment_uploads,
            author_id as comment_author_user_id,
            audit_id,
            body as comment_body_string,
            html_body as comment_html_body,
            plain_body as comment_plain_body,
            "timestamp" as comment_unix_timestamp,
            nullif(attachments, array[]) as comment_attachments,
            nullif(json_query(via, 'lax $.channel' omit quotes), 'null') as comment_source_channel,
            nullif(json_query(via, 'lax $.source.from.address' omit quotes), 'null') as comment_source_email,
            nullif(json_query(via, 'lax $.source.rel' omit quotes), 'null') as comment_source_rel,
            {{ cast_timestamp_to_iso8601("created_at") }} as comment_created_at
        from most_recent_source
    )

select *
from cleaned
