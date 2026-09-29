with source as (
    select *
    from {{ source('ol_warehouse_raw_data', 'raw__mitlearn__app__postgres__learning_resources_learningresourcetopicmapping') }}
)

-- the raw stream can hold more than one row per id; keep the most recently extracted
{{ deduplicate_raw_table(raw_table='raw__mitlearn__app__postgres__learning_resources_learningresourcetopicmapping', partition_columns='id') }}

, cleaned as (
    select
        id as learningresourcetopicmapping_id
        , topic_id as learningresourcetopic_id
        , offeror_id as learningresourceofferor_code
        , topic_name as learningresourcetopicmapping_offeror_topic_name
    from most_recent_source
)

select * from cleaned
