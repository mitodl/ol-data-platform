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

-- A retired learner's posts are not feedback to keep. The forum overwrites them with
-- '[deleted]' at retirement, but posts copied to MySQL before a retirement that ran
-- against Mongo still hold the original text, so the LMS rename is what is trusted.
-- A row the forum did overwrite carries user_retired_username, which also covers a
-- retirement that stopped before the LMS rename.
, retired_users as (
    select openedx_user_id
    from {{ ref('stg__mitxonline__openedx__mysql__auth_user') }}
    where substr(user_username, 1, 14) = 'retired__user_'
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

-- The Mongo-to-MySQL migration (2025-09-18/19) stamped every copied post with its own
-- run time. The original Mongo ObjectId still encodes the real creation time in its
-- first 8 hex characters (Unix seconds).
, mongo_created as (
    select
        content_type.contenttype_model as content_model
        , mongo_content.forumcontent_object_id as object_id
        , {{ cast_timestamp_to_iso8601(
            mongo_objectid_timestamp("mongo_content.forumcontent_mongo_id")
        ) }} as created_on
    from {{ ref('stg__mitxonline__openedx__mysql__forum_mongocontent') }} as mongo_content
    inner join {{ ref('stg__mitxonline__openedx__mysql__django_content_type') }} as content_type
        on mongo_content.forumcontent_type_id = content_type.contenttype_id
    where
        content_type.contenttype_app_label = 'forum'
        and content_type.contenttype_model in ('commentthread', 'comment')
        and {{ regexp_like("mongo_content.forumcontent_mongo_id", "'^[0-9a-f]{24}$'") }}
)

, posts as (
    select
        {{ dbt.concat(["'commentthread-'", "cast(forumthread_id as varchar)"]) }} as source_record_ref
        , 'commentthread' as content_model
        , forumthread_id as object_id
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
        , user_retired_username is not null as is_forum_retired
    from forum_thread

    union all

    select
        {{ dbt.concat(["'comment-'", "cast(forumcomment_id as varchar)"]) }} as source_record_ref
        , 'comment' as content_model
        , forumcomment_id as object_id
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
        , user_retired_username is not null as is_forum_retired
    from forum_comment
)

-- Thread-level windows run here, before the staff filter, so they see every post
, dated_posts as (
    select
        posts.source_record_ref
        , posts.forumthread_id
        , posts.user_id
        , posts.courserun_readable_id
        , posts.post_type
        , posts.post_type_order
        , posts.is_anonymous
        , posts.is_visible
        , posts.is_forum_retired
        , coalesce(mongo_created.created_on, posts.post_created_on) as post_created_on
        -- Bodies are HTML; the LLM and Presidio should see plain text
        , trim({{ regexp_replace_all(
            html_unescape(
                "replace(" ~ regexp_replace_all("posts.post_body", "'<[^>]+>'", "' '") ~ ", '&nbsp;', ' ')"
            ),
            "'\\s+'",
            "' '"
        ) }}) as post_body
        -- Conversation-level, so a new reply re-enters the whole thread under
        -- tfact_feedback's watermark
        , max(posts.post_updated_on) over (partition by posts.forumthread_id)
            as thread_updated_on
    from posts
    left join mongo_created
        on
            posts.content_model = mongo_created.content_model
            and posts.object_id = mongo_created.object_id
)

, learner_turns as (
    select dated_posts.*
    from dated_posts
    left join course_staff
        on
            dated_posts.user_id = course_staff.openedx_user_id
            and dated_posts.courserun_readable_id = course_staff.courserun_readable_id
    left join retired_users
        on dated_posts.user_id = retired_users.openedx_user_id
    where
        course_staff.openedx_user_id is null
        and retired_users.openedx_user_id is null
        and not dated_posts.is_forum_retired
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
        and nullif(post_body, '') is not null
)

select
    'discussion_forum' as source_slug
    , numbered_turns.post_created_on as occurred_at
    , numbered_turns.source_record_ref
    -- The title often carries the whole question ("images won't load" over a body of
    -- "eom"), and the summarizer reads only text, so it leads the first kept turn.
    , case
        when
            numbered_turns.turn_index = 1
            and forum_thread.forumthread_title is not null
            and thread_author_retired.openedx_user_id is null
            and forum_thread.user_retired_username is null
            then {{ dbt.concat(["forum_thread.forumthread_title", codepoint_char(10), codepoint_char(10), "numbered_turns.post_body"]) }}
        else numbered_turns.post_body
    end as text
    -- Other learners' replies stay, without the retired author's title over them
    , case
        when
            thread_author_retired.openedx_user_id is null
            and forum_thread.user_retired_username is null
            then forum_thread.forumthread_title
    end as title
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
    , {{ json_object_from_pairs([
        ['post_type', "numbered_turns.post_type"]
        , ['thread_type', "forum_thread.forumthread_type"]
        , ['commentable_id', "forum_thread.forumthread_commentable_id"]
        , ['courserun_platform', "'mitxonline'"]
    ]) }} as source_metadata
from numbered_turns
inner join forum_thread
    on numbered_turns.forumthread_id = forum_thread.forumthread_id
left join users
    on numbered_turns.user_id = users.openedx_user_id
left join retired_users as thread_author_retired
    on forum_thread.user_id = thread_author_retired.openedx_user_id
left join discussion_block
    on
        forum_thread.courserun_readable_id = discussion_block.courserun_readable_id
        and forum_thread.forumthread_commentable_id = discussion_block.commentable_id
-- A hidden thread hides its replies too, whatever their own flag says
where forum_thread.forumthread_is_visible = true
