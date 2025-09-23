with websitecontents as (select * from {{ ref("stg__ocw__studio__postgres__websites_websitecontent") }})

select distinct  -- a few courses have duplicated topic, subtopic and speciality combination
    websitecontents.website_uuid as course_uuid,
    element_at(course_topic, 1) as course_topic,  -- noqa
    element_at(course_topic, 2) as course_subtopic,  -- noqa
    element_at(course_topic, 3) as course_speciality  -- noqa
from websitecontents
cross join unnest(cast(json_parse(course_topics) as array(array(varchar)))) as t(course_topic)  -- noqa
where websitecontents.websitecontent_type = 'sitemetadata'
