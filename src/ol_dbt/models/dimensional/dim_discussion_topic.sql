with discussion_component_topics as (
    select
        *
        , json_query(block_metadata, 'lax $.discussion_category' omit quotes) as discussion_component_category
        , json_query(block_metadata, 'lax $.discussion_target' omit quotes) as discussion_component_name
        , json_query(block_metadata, 'lax $.discussion_id' omit quotes) as discussion_component_id
    from {{ ref('dim_course_content') }}
    where
        block_category = 'discussion'
        and is_latest = true
)

, course_level_discussion_topics as (
    select
        course.*
        , t.key as topic_name -- noqa
        , json_extract_scalar(t.topic, '$.id') as topic_id -- noqa
        , row_number() over (
            partition by course.block_id, json_extract_scalar(t.topic, '$.id') -- noqa
            order by json_extract_scalar(t.topic, '$.sort_key') asc -- noqa
        ) as row_num
    from {{ ref('dim_course_content') }} as course
    cross join unnest(cast(json_extract(course.block_metadata, '$.discussion_topics') as map(varchar, json))) AS t(key, topic) -- noqa
    where course.block_category = 'course'
    and course.is_latest = true

)

, combined as (
    select
        content_block_pk as content_block_fk
        , block_id as discussion_block_pk
        , courserun_readable_id
        , discussion_component_name as topic_name
        , discussion_component_id as commentable_id
        , discussion_component_category as category_name
        , 'discussion component' as discussion_type
    from discussion_component_topics

    union all

    select
        content_block_pk as content_block_fk
        , block_id as discussion_block_pk
        , courserun_readable_id
        , topic_name
        , topic_id as commentable_id
        , topic_name as category_name
        , 'course-wide discussion' as discussion_type
    from course_level_discussion_topics
    where row_num = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['discussion_block_pk', 'commentable_id']) }} as discussion_topic_pk
    , content_block_fk
    , discussion_block_pk
    , courserun_readable_id
    , commentable_id
    , topic_name
    , category_name
    , discussion_type
from combined
