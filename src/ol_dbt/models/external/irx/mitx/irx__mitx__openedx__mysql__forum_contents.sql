-- One row per forum post. Threads and comments were unified under this shape while the
-- forum still ran on Mongo; the IRx export now delivers it as-is (forum_contents.parquet),
-- not reconstructed into a Mongo document, since Mongo has not backed the forum since the
-- forum-v2 cutover.
--
-- The generic foreign keys (content_type_id, content_object_id) are resolved by content
-- type name, never by id: the ids differ per deployment, and thread and comment ids both
-- start at 1, so an unfiltered join silently matches both tables.
--
-- mongoid and the *_mongoid references come from forum_mongocontent, which only has rows
-- for content migrated out of Mongo; they are null for content created after the cutover.
-- Kept for historical continuity with the legacy ObjectIds, not manufactured.

with content_types as (
    select
        id
        , model
    from {{ source('ol_warehouse_raw_data', 'raw__mitx__openedx__mysql__django_content_type') }}
    where app_label = 'forum' and model in ('commentthread', 'comment')
)

, bridge as (
    select
        content_types.model
        , mongocontent.content_object_id
        , mongocontent.mongo_id
    from {{ source('ol_warehouse_raw_data', 'raw__mitx__openedx__mysql__forum_mongocontent') }} as mongocontent
    inner join content_types on mongocontent.content_type_id = content_types.id
)

, votes as (
    select
        content_types.model
        , uservote.content_object_id
        , array_agg(cast(uservote.user_id as varchar) order by uservote.id)
            filter (where uservote.vote > 0) as votes_up
        , array_agg(cast(uservote.user_id as varchar) order by uservote.id)
            filter (where uservote.vote < 0) as votes_down
    from {{ source('ol_warehouse_raw_data', 'raw__mitx__openedx__mysql__forum_uservote') }} as uservote
    inner join content_types on uservote.content_type_id = content_types.id
    group by content_types.model, uservote.content_object_id
)

, abuse_flaggers as (
    select
        content_types.model
        , flagger.content_object_id
        , array_agg(cast(flagger.user_id as varchar) order by flagger.id) as user_ids
    from {{ source('ol_warehouse_raw_data', 'raw__mitx__openedx__mysql__forum_abuseflagger') }} as flagger
    inner join content_types on flagger.content_type_id = content_types.id
    group by content_types.model, flagger.content_object_id
)

, historical_abuse_flaggers as (
    select
        content_types.model
        , flagger.content_object_id
        , array_agg(cast(flagger.user_id as varchar) order by flagger.id) as user_ids
    from {{ source('ol_warehouse_raw_data', 'raw__mitx__openedx__mysql__forum_historicalabuseflagger') }} as flagger
    inner join content_types on flagger.content_type_id = content_types.id
    group by content_types.model, flagger.content_object_id
)

, comments as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitx__openedx__mysql__forum_comment') }}
)

, comment_counts as (
    select
        comment_thread_id
        , count(*) as comment_count
    from comments
    group by comment_thread_id
)

, threads as (
    select
        'CommentThread' as _type
        , thread.id
        , bridge.mongo_id as mongoid
        , cast(null as bigint) as comment_thread_id
        , cast(null as varchar) as comment_thread_mongoid
        , cast(null as bigint) as parent_id
        , cast(null as varchar) as parent_mongoid
        , thread.course_id
        , thread.author_id
        , thread.author_username
        , thread.title
        , thread.body
        , thread.thread_type
        , thread.context
        , thread.commentable_id
        , thread.group_id
        , thread.closed
        , thread.pinned
        , thread.endorsed
        , cast(null as varchar) as endorsement
        , thread.visible
        , thread.anonymous
        , thread.anonymous_to_peers
        , cast(null as bigint) as depth
        , cast(null as bigint) as child_count
        , coalesce(comment_counts.comment_count, 0) as comment_count
        , thread.created_at
        , thread.updated_at
        , thread.last_activity_at
        , votes.votes_up
        , votes.votes_down
        , abuse_flaggers.user_ids as abuse_flaggers
        , historical_abuse_flaggers.user_ids as historical_abuse_flaggers
    from {{ source('ol_warehouse_raw_data', 'raw__mitx__openedx__mysql__forum_commentthread') }} as thread
    left join bridge
        on bridge.model = 'commentthread' and thread.id = bridge.content_object_id
    left join comment_counts on thread.id = comment_counts.comment_thread_id
    left join votes
        on votes.model = 'commentthread' and thread.id = votes.content_object_id
    left join abuse_flaggers
        on abuse_flaggers.model = 'commentthread' and thread.id = abuse_flaggers.content_object_id
    left join historical_abuse_flaggers
        on
            historical_abuse_flaggers.model = 'commentthread'
            and thread.id = historical_abuse_flaggers.content_object_id
)

, responses_and_comments as (
    select
        'Comment' as _type
        , comments.id
        , bridge.mongo_id as mongoid
        , comments.comment_thread_id
        , thread_bridge.mongo_id as comment_thread_mongoid
        , comments.parent_id
        , parent_bridge.mongo_id as parent_mongoid
        , comments.course_id
        , comments.author_id
        , comments.author_username
        , cast(null as varchar) as title
        , comments.body
        , cast(null as varchar) as thread_type
        , cast(null as varchar) as context
        , cast(null as varchar) as commentable_id
        , comments.group_id
        , cast(null as boolean) as closed
        , cast(null as boolean) as pinned
        , comments.endorsed
        , comments.endorsement
        , comments.visible
        , comments.anonymous
        , comments.anonymous_to_peers
        , comments.depth
        , comments.child_count
        , cast(null as bigint) as comment_count
        , comments.created_at
        , comments.updated_at
        , cast(null as timestamp) as last_activity_at
        , votes.votes_up
        , votes.votes_down
        , abuse_flaggers.user_ids as abuse_flaggers
        , historical_abuse_flaggers.user_ids as historical_abuse_flaggers
    from comments
    left join bridge
        on bridge.model = 'comment' and comments.id = bridge.content_object_id
    left join bridge as thread_bridge
        on thread_bridge.model = 'commentthread' and comments.comment_thread_id = thread_bridge.content_object_id
    left join bridge as parent_bridge
        on parent_bridge.model = 'comment' and comments.parent_id = parent_bridge.content_object_id
    left join votes
        on votes.model = 'comment' and comments.id = votes.content_object_id
    left join abuse_flaggers
        on abuse_flaggers.model = 'comment' and comments.id = abuse_flaggers.content_object_id
    left join historical_abuse_flaggers
        on
            historical_abuse_flaggers.model = 'comment'
            and comments.id = historical_abuse_flaggers.content_object_id
)

select * from threads
union all
select * from responses_and_comments
