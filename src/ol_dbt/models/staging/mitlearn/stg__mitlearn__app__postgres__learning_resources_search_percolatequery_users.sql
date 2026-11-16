with source as (
    {{ live_raw_rows('raw__mitlearn__app__postgres__learning_resources_search_percolatequery_users') }} -- noqa: LT05
)

, cleaned as (
    select
        id as percolatequery_users_id
        , user_id
        , percolatequery_id
    from source
)

select * from cleaned
