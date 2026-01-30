{{ config(
    materialized='table'
) }}

-- Map courses to topics (many-to-many)
-- Note: OCW courses not yet in dim_course (Phase 1-2), so omitted here
with mitxonline_course_topics as (
    select
        course_id
        , coursetopic_name as topic_name
        , '{{ var("mitxonline") }}' as platform
    from {{ ref('int__mitxonline__course_to_topics') }}
)

, mitxpro_course_topics as (
    select
        course_id
        , coursetopic_name as topic_name
        , '{{ var("mitxpro") }}' as platform
    from {{ ref('int__mitxpro__courses_to_topics') }}
)

, combined_course_topics as (
    select * from mitxonline_course_topics
    union all
    select * from mitxpro_course_topics
)

-- Join to dimensions to get FKs
, dim_course as (
    select course_pk, source_id, primary_platform
    from {{ ref('dim_course') }}
    where is_current = true
)

, dim_topic as (
    select topic_pk, topic_name
    from {{ ref('dim_topic') }}
)

, bridge as (
    select
        dim_course.course_pk as course_fk
        , dim_topic.topic_pk as topic_fk
    from combined_course_topics
    inner join dim_course
        on combined_course_topics.course_id = dim_course.source_id
        and combined_course_topics.platform = dim_course.primary_platform
    inner join dim_topic
        on combined_course_topics.topic_name = dim_topic.topic_name
)

-- Deduplicate in case same topic-course pair exists multiple times
select distinct
    course_fk
    , topic_fk
from bridge
