with source as (
    select *
    from {{ source('ol_warehouse_raw_data', 'raw__mitlearn__app__postgres__profiles_profile_topic_interests') }}
)

, cleaned as (
    select
        id as profiletopicinterests_id
        , profile_id
        , learningresourcetopic_id
    from source
)

select * from cleaned
