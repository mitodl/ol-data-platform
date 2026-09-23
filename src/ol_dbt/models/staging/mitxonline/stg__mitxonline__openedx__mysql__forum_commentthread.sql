-- MITx Online open edX discussion forum threads

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__openedx__mysql__forum_commentthread') }}
)

, cleaned as (

    select
        id as forumthread_id
        , author_id as user_id
        , author_username as user_username
        , retired_username as user_retired_username
        , course_id as courserun_readable_id
        , title as forumthread_title
        , body as forumthread_body
        , thread_type as forumthread_type
        , commentable_id as forumthread_commentable_id
        , context as forumthread_context
        , group_id as forumthread_group_id
        , closed as forumthread_is_closed
        , closed_by_id as forumthread_closed_by_user_id
        , close_reason_code as forumthread_close_reason_code
        , pinned as forumthread_is_pinned
        , visible as forumthread_is_visible
        , endorsed as forumthread_is_endorsed
        , anonymous as forumthread_is_anonymous
        , anonymous_to_peers as forumthread_is_anonymous_to_peers
        , {{ cast_timestamp_to_iso8601('created_at') }} as forumthread_created_on
        , {{ cast_timestamp_to_iso8601('updated_at') }} as forumthread_updated_on
        , {{ cast_timestamp_to_iso8601('last_activity_at') }} as forumthread_last_activity_on
    from source
)

select * from cleaned
