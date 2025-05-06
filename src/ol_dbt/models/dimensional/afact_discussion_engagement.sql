with discussion_events as (
    select * from {{ ref('tfact_discussion_events') }}
)

, discussion_topics as (
    select * from {{ ref('dim_discussion_topic') }}
)

, course_content as (
    select * from {{ ref('dim_course_content') }}
)

, aggregated as (
    select
        discussion_events.platform_fk
        , discussion_events.courserun_readable_id
        , discussion_events.openedx_user_id
        , discussion_events.commentable_id
        , discussion_events.post_id
        , arbitrary(discussion_events.platform) as platform
        , arbitrary(discussion_events.user_fk) as user_fk
        , arbitrary(discussion_events.post_title) as post_title
        , count_if(discussion_events.event_type = 'edx.forum.thread.created') as post_created
        , count_if(discussion_events.event_type = 'edx.forum.thread.viewed') as post_viewed
        , count_if(discussion_events.event_type = 'edx.forum.thread.voted') as post_voted
        , count_if(discussion_events.event_type = 'edx.forum.response.created') as post_replied
        , count_if(discussion_events.event_type = 'edx.forum.response.voted') as response_voted
        , count_if(discussion_events.event_type = 'edx.forum.comment.created') as post_commented
    from discussion_events
    where discussion_events.commentable_id is not null
    group by
        discussion_events.platform_fk
        , discussion_events.openedx_user_id
        , discussion_events.courserun_readable_id
        , discussion_events.commentable_id
        , discussion_events.post_id
)

select
    aggregated.platform_fk
    , aggregated.courserun_readable_id
    , aggregated.openedx_user_id
    , aggregated.commentable_id
    , aggregated.post_id
    , aggregated.platform
    , aggregated.user_fk
    , aggregated.post_title
    , aggregated.post_created
    , aggregated.post_viewed
    , aggregated.post_voted
    , aggregated.post_replied
    , aggregated.response_voted
    , aggregated.post_commented
    , discussion_topics.discussion_topic_pk as discussion_topic_fk
    , discussion_topics.discussion_type
    , discussion_topics.content_block_fk
    , course_content.sequential_block_id as sequential_block_fk
    , course_content.chapter_block_id as chapter_block_fk
from aggregated
left join discussion_topics
    on
        aggregated.commentable_id = discussion_topics.commentable_id
        and aggregated.courserun_readable_id = discussion_topics.courserun_readable_id
left join course_content
    on discussion_topics.content_block_fk = course_content.content_block_pk
