-- MITxPro Course to Topic Information
-- Keep it as separate model for flexibility to satisfy different use cases

with topics as (
    select * from {{ ref('stg__mitxpro__app__postgres__courses_coursetopic') }}
)

, course_to_topics as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__courses_course_to_topic') }}
)

select
    course_to_topics.course_id
    , topics.coursetopic_name
from course_to_topics
inner join topics on course_to_topics.coursetopic_id = topics.coursetopic_id
