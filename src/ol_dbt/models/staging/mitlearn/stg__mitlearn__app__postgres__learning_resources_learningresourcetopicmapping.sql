with source as (
    select *
    from {{ source('ol_warehouse_raw_data', 'raw__mitlearn__app__postgres__learning_resources_learningresourcetopicmapping') }}
)

, cleaned as (
    select
        id as learningresourcetopicmapping_id
        , topic_id as learningresourcetopic_id
        , offeror_id as learningresourceofferor_code
        , topic_name as learningresourcetopicmapping_offeror_topic_name
    from source
)

select * from cleaned
