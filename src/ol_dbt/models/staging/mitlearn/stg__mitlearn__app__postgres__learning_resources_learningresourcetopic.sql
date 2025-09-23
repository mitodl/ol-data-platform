with
    source as (
        select *
        from
            {{
                source(
                    "ol_warehouse_raw_data", "raw__mitlearn__app__postgres__learning_resources_learningresourcetopic"
                )
            }}
    ),
    cleaned as (
        select
            id as learningresourcetopic_id,
            name as learningresourcetopic_name,
            parent_id as learningresourcetopic_parent_id,
            topic_uuid as learningresourcetopic_uuid
        from source
    )

select *
from cleaned
