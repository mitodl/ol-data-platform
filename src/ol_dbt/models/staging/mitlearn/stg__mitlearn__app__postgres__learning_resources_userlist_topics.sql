with source as (
    {{ live_raw_rows('raw__mitlearn__app__postgres__learning_resources_userlist_topics') }}
)

, cleaned as (
    select
        id as userlisttopics_id
        , userlist_id
        , learningresourcetopic_id
    from source
)

select * from cleaned
