{{ config(
    materialized='table'
) }}

select
    course_readable_id
    , course_name
    , course_term
    , course_year
    , course_level
    , course_extra_course_numbers
    , course_is_unpublished
    , course_has_never_published
    , course_live_url
    , course_first_published_on
    , course_publish_date_updated_on
    , course_topics
    , course_learning_resource_types
    , course_department_numbers
from {{ ref('int__ocw__courses') }}
