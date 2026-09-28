-- MITx Pro open edX discussion forum responses and comments

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__openedx__mysql__forum_comment') }}
)

, cleaned as (

    select
        id as forumcomment_id
        , comment_thread_id as forumthread_id
        , parent_id as forumcomment_parent_id
        , author_id as user_id
        , author_username as user_username
        , retired_username as user_retired_username
        , course_id as courserun_readable_id
        , body as forumcomment_body
        , depth as forumcomment_depth
        , sort_key as forumcomment_sort_key
        , child_count as forumcomment_child_count
        , endorsement as forumcomment_endorsement_data
        , endorsed as forumcomment_is_endorsed
        , visible as forumcomment_is_visible
        , anonymous as forumcomment_is_anonymous
        , anonymous_to_peers as forumcomment_is_anonymous_to_peers
        , group_id as forumcomment_group_id
        , {{ cast_timestamp_to_iso8601('created_at') }} as forumcomment_created_on
        , {{ cast_timestamp_to_iso8601('updated_at') }} as forumcomment_updated_on
    from source
)

select * from cleaned
