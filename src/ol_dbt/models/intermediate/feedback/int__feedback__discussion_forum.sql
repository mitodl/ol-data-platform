-- Grain: one learner-authored thread post, response or comment = one turn of its thread.
with forum_thread as (
    select * from {{ ref('stg__mitxonline__openedx__mysql__forum_commentthread') }}
)

, forum_comment as (
    select * from {{ ref('stg__mitxonline__openedx__mysql__forum_comment') }}
)

, users as (
    select
        openedx_user_id
        , user_global_id
        , user_email
    from {{ ref('int__mitxonline__users') }}
    where openedx_user_id is not null
)

-- Course staff answering is the platform, not feedback, the same rule that drops
-- Zendesk agent replies. Other roles (beta testers, data researchers) are kept.
, course_staff as (
    select distinct
        openedx_user_id
        , courserun_readable_id
    from {{ ref('stg__mitxonline__openedx__mysql__user_courseaccessrole') }}
    where courseaccess_role in ('staff', 'instructor')
)

-- An inline discussion's commentable_id is its block's discussion_id. A course run can
-- reuse one discussion_id across units, so those stay unresolved rather than guessed.
, discussion_block as (
    select
        courserun_readable_id
        , commentable_id
        , min(discussion_block_pk) as block_id
    from {{ ref('dim_discussion_topic') }}
    where
        platform = 'mitxonline'
        and discussion_type = 'discussion component'
    group by courserun_readable_id, commentable_id
    having count(*) = 1
)

, posts as (
    select
        'commentthread-' || cast(forumthread_id as varchar) as source_record_ref
        , forumthread_id
        , user_id
        , courserun_readable_id
        , forumthread_body as post_body
        , 'thread' as post_type
        , 0 as post_type_order
        , forumthread_is_anonymous or forumthread_is_anonymous_to_peers as is_anonymous
        , forumthread_is_visible as is_visible
        , forumthread_created_on as post_created_on
        , forumthread_updated_on as post_updated_on
    from forum_thread

    union all

    select
        'comment-' || cast(forumcomment_id as varchar) as source_record_ref
        , forumthread_id
        , user_id
        , courserun_readable_id
        , forumcomment_body as post_body
        , case when forumcomment_depth = 0 then 'response' else 'comment' end as post_type
        , 1 as post_type_order
        , forumcomment_is_anonymous or forumcomment_is_anonymous_to_peers as is_anonymous
        , forumcomment_is_visible as is_visible
        , forumcomment_created_on as post_created_on
        , forumcomment_updated_on as post_updated_on
    from forum_comment
)

, learner_turns as (
    select
        posts.*
        -- Conversation-level, so a new reply re-enters the whole thread under
        -- tfact_feedback's watermark. Taken over every post, staff included.
        , max(posts.post_updated_on) over (partition by posts.forumthread_id)
            as thread_updated_on
    from posts
    left join course_staff
        on
            posts.user_id = course_staff.openedx_user_id
            and posts.courserun_readable_id = course_staff.courserun_readable_id
    where course_staff.openedx_user_id is null
)

, numbered_turns as (
    select
        *
        , row_number() over (
            partition by forumthread_id
            order by post_created_on, post_type_order, source_record_ref
        ) as turn_index
    from learner_turns
    where
        is_visible = true
        and nullif(trim(post_body), '') is not null
)

select
    'discussion_forum' as source_slug
    , numbered_turns.post_created_on as occurred_at
    , numbered_turns.source_record_ref
    , numbered_turns.post_body as text
    , forum_thread.forumthread_title as title
    , cast(numbered_turns.forumthread_id as varchar) as conversation_ref
    , numbered_turns.turn_index
    , numbered_turns.turn_index = 1 as is_conversation_opening
    -- An anonymous post stays anonymous here too
    , case
        when not numbered_turns.is_anonymous
            then coalesce(users.user_global_id, users.user_email)
    end as subject_user_ref
    , cast(null as varchar) as source_url
    , 'forum_post' as channel_slug
    , numbered_turns.courserun_readable_id
    , discussion_block.block_id
    -- MITx Online's Open edX is MIT Learn's course backend
    , 'mitlearn' as platform
    , case when discussion_block.block_id is not null then 'courseware_block' else 'course' end
        as subject_type
    , coalesce(discussion_block.block_id, numbered_turns.courserun_readable_id) as subject_ref
    , cast(null as varchar) as subject_url
    , cast(null as varchar) as explicit_rating
    , numbered_turns.post_created_on as created_at
    , numbered_turns.thread_updated_on as updated_at
    , json_object(
        'post_type': numbered_turns.post_type
        , 'thread_type': forum_thread.forumthread_type
        , 'commentable_id': forum_thread.forumthread_commentable_id
        , 'courserun_platform': 'mitxonline'
    ) as source_metadata
from numbered_turns
inner join forum_thread
    on numbered_turns.forumthread_id = forum_thread.forumthread_id
left join users
    on numbered_turns.user_id = users.openedx_user_id
left join discussion_block
    on
        forum_thread.courserun_readable_id = discussion_block.courserun_readable_id
        and forum_thread.forumthread_commentable_id = discussion_block.commentable_id
