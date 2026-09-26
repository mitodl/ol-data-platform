with source as (
    select *
    from {{ source('ol_warehouse_raw_data', 'raw__mitlearn__app__postgres__learning_resources_learningresourcetopic') }}
)

-- the raw stream can hold more than one row per id; keep the most recently extracted
{{ deduplicate_raw_table(raw_table='raw__mitlearn__app__postgres__learning_resources_learningresourcetopic', partition_columns='id') }}

, cleaned as (
    select
        id as learningresourcetopic_id
        , name as learningresourcetopic_name
        , parent_id as learningresourcetopic_parent_id
        , topic_uuid as learningresourcetopic_uuid
    from most_recent_source
)

select * from cleaned
