with source as (
    select *
    from {{ source('ol_warehouse_raw_data', 'raw__mitlearn__app__postgres__learning_resources_userlistrelationship') }}
)

, cleaned as (
    select
        id as userlistrelationship_id
        , child_id as userlistrelationship_child_id
        , position as userlistrelationship_position
        , parent_id as userlistrelationship_parent_id
        , created_on as userlistrelationship_created_on
        , updated_on as userlistrelationship_updated_on
    from source
)

select * from cleaned
