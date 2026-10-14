with websitecontents as (
    select * from {{ ref('stg__ocw__studio__postgres__websites_websitecontent') }}
)

select distinct -- a few courses have duplicated topic, subtopic and speciality combination
    websitecontents.website_uuid as course_uuid
    , {{ element_at_array('course_topic', 1) }} as course_topic -- noqa
    , {{ element_at_array('course_topic', 2) }} as course_subtopic -- noqa
    , {{ element_at_array('course_topic', 3) }} as course_speciality -- noqa
from websitecontents
cross join unnest({{ json_extract_nested_varchar_array('course_topics', "'$'") }}) as t(course_topic) -- noqa
where websitecontents.websitecontent_type = 'sitemetadata'
