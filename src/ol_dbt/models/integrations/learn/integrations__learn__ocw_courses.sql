{#
  integrations__learn__ocw_courses
  Exposes OCW courses for MIT Learn's Trino-pull ETL.
  Contract: docs/learn_marts_contract.md
#}

with courses as (
    select * from {{ ref('int__ocw__courses') }}
)

, course_topics as (
    select
        course_uuid
        , array_join(
            array_agg(
                coalesce(
                    course_speciality,
                    coalesce(course_subtopic, course_topic)
                )
            ),
            ', '
        ) as course_topics_flat
    from {{ ref('int__ocw__course_topics') }}
    group by course_uuid
)

, course_instructors as (
    select
        course_uuid
        , array_join(
            array_agg(
                concat(
                    coalesce(concat(course_instructor_salutation, ' '), ''),
                    coalesce(concat(course_instructor_first_name, ' '), ''),
                    coalesce(concat(course_instructor_middle_initial, ' '), ''),
                    coalesce(course_instructor_last_name, '')
                )
            ),
            ', '
        ) as course_instructors
    from {{ ref('int__ocw__course_instructors') }}
    group by course_uuid
)

, departments as (
    select
        course_uuid
        , array_join(array_agg(course_department_name), ', ') as course_departments
    from {{ ref('int__ocw__course_departments') }}
    group by course_uuid
)

select
    courses.course_readable_id                              as readable_id
    , courses.course_title                                  as title
    , coalesce(
        courses.course_publish_date_updated_on,
        courses.course_updated_on
    )                                                       as last_modified
    , 'ocw'                                                 as etl_source
    , courses.course_description                            as description
    , courses.course_live_url                               as url
    , null                                                  as image_url
    , courses.course_is_live                                as published
    , 'ocw'                                                 as platform
    , courses.course_level                                  as level
    , courses.course_term                                   as term
    , courses.course_year                                   as year
    , courses.course_primary_course_number                  as course_number
    , courses.course_extra_course_numbers                   as extra_course_numbers
    , course_topics.course_topics_flat                      as topics
    , course_instructors.course_instructors                 as instructors
    , departments.course_departments                        as departments
from courses
left join course_topics
    on courses.course_uuid = course_topics.course_uuid
left join course_instructors
    on courses.course_uuid = course_instructors.course_uuid
left join departments
    on courses.course_uuid = departments.course_uuid
where courses.course_is_live = true
