-- Grain: one commented submission = one single-turn conversation. Sentiment-only
-- submissions carry no text to redact, summarize or embed, so they are dropped.
with content_feedback as (
    select * from {{ ref('stg__mitlearn__app__postgres__content_feedback_contentfeedback') }}
)

, users as (
    select
        user_id
        , user_global_id
        , user_email
    from {{ ref('stg__mitlearn__app__postgres__users_user') }}
)

-- Same rule as int__feedback__learn_ai_tutor: edxorg and mitxonline share some
-- readable ids, and MIT Learn serves MITx Online courses, so prefer it.
, course_run as (
    select
        courserun_readable_id
        , case
            when count(distinct platform) = 1 then min(platform)
            when count_if(platform = 'mitxonline') > 0 then 'mitxonline'
        end as platform
    from {{ ref('dim_course_run') }}
    group by courserun_readable_id
)

select
    'content_feedback' as source_slug
    , content_feedback.contentfeedback_created_on as occurred_at
    , cast(content_feedback.contentfeedback_id as varchar) as source_record_ref
    , content_feedback.contentfeedback_comment as text
    -- Block and unit names are course metadata, not learner text, so they go in
    -- source_metadata instead of through redaction as a title.
    , cast(null as varchar) as title
    , cast(content_feedback.contentfeedback_id as varchar) as conversation_ref
    , 1 as turn_index
    , true as is_conversation_opening
    -- The event contract allows email only when the user has no global id
    , coalesce(users.user_global_id, users.user_email) as subject_user_ref
    , cast(null as varchar) as source_url
    , 'in_product_widget' as channel_slug
    , content_feedback.courserun_readable_id
    , content_feedback.contentfeedback_block_usage_key as block_id
    , 'mitlearn' as platform
    , 'courseware_block' as subject_type
    , content_feedback.contentfeedback_block_usage_key as subject_ref
    , content_feedback.contentfeedback_url as subject_url
    , content_feedback.contentfeedback_sentiment as explicit_rating
    , content_feedback.contentfeedback_created_on as created_at
    , content_feedback.contentfeedback_updated_on as updated_at
    , json_object(
        'courserun_title': content_feedback.courserun_title
        , 'block_type': content_feedback.contentfeedback_block_type
        , 'block_display_name': content_feedback.contentfeedback_block_display_name
        , 'unit_title': content_feedback.contentfeedback_unit_title
        , 'courserun_platform': course_run.platform
    ) as source_metadata
from content_feedback
left join users
    on content_feedback.user_id = users.user_id
left join course_run
    on content_feedback.courserun_readable_id = course_run.courserun_readable_id
where content_feedback.contentfeedback_comment is not null
