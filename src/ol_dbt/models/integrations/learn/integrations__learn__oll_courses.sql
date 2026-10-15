{#
  integrations__learn__oll_courses
  Open Learning Library courses for MIT Learn webhook delivery, one row per row of
  the OLL sheet, shaped as MIT Learn's legacy OLL ETL (learning_resources/etl/oll.py)
  writes them. Each course has exactly one run, the sheet row's edX run key.
  Contract: docs/learn_marts_contract.md

  OCW-offered rows stay in: they are OLL-hosted runs (openlearninglibrary.mit.edu)
  that MIT Learn lists as offered by OCW, and none is the OCW course site that
  integrations__learn__ocw_courses carries.
#}

with courses as (
    select * from {{ ref('stg__oll__google_sheets__courses') }}
    -- MIT Learn's SKIP_OCW_COURSES: runs its OCW pipeline already ingests
    where coalesce(courserun_readable_id, '') != 'OCW+18.031+2019_Spring'
)

-- get_year_and_semester: a 1T/2T/3T term key first, then a 2022_Summer style suffix
, run_terms as (
    select
        courserun_readable_id
        , {{ regexp_extract_or_null('courserun_readable_id', "'[1|2|3]T[0-9]{4}'") }} as term_key
        , {{ regexp_extract_or_null('courserun_readable_id', "'([0-9]{4})_(Spring|Summer|Fall|Winter)'", 1) }}
            as suffix_year
        , {{ regexp_extract_or_null('courserun_readable_id', "'([0-9]{4})_(Spring|Summer|Fall|Winter)'", 2) }}
            as suffix_semester
    from courses
)

, runs as (
    select
        courserun_readable_id
        , case
            when term_key is not null then cast(substring(term_key, 3, 4) as integer)
            else cast(suffix_year as integer)
        end as year
        , case
            when term_key is not null
                then
                    case substring(term_key, 1, 2)
                        when '1T' then 'Spring'
                        when '2T' then 'Summer'
                        when '3T' then 'Fall'
                    end
            else suffix_semester
        end as semester
    from run_terms
)

, instructor_names as (
    select courserun_readable_id, 1 as position, instructor_1 as instructor_name from courses
    union all
    select courserun_readable_id, 2 as position, instructor_2 as instructor_name from courses
    union all
    select courserun_readable_id, 3 as position, instructor_3 as instructor_name from courses
    union all
    select courserun_readable_id, 4 as position, instructor_4 as instructor_name from courses
    union all
    select courserun_readable_id, 5 as position, instructor_5 as instructor_name from courses
    union all
    select courserun_readable_id, 6 as position, instructor_6 as instructor_name from courses
)

, instructors as (
    select
        courserun_readable_id
        , array_agg(instructor_name order by position) as instructors
    from instructor_names
    where instructor_name != ''
    group by courserun_readable_id
)

-- Names are kept as the sheet has them, trailing spaces included: MIT Learn looks
-- topics up by exact name, so the legacy ETL drops the same ones this does.
, topic_names as (
    select courserun_readable_id, 1 as position, course_primary_topic as topic_name from courses
    union all
    select courserun_readable_id, 2 as position, course_secondary_topic as topic_name from courses
)

, topics as (
    select
        courserun_readable_id
        , array_agg(replace(topic_name, 'Educational Policy', 'Education Policy') order by position) as topics
    from topic_names
    where topic_name != ''
    group by courserun_readable_id
)

, shaped as (
    select
        courses.course_number
        , courses.courserun_readable_id
        , courses.course_title
        , courses.course_url
        , courses.course_description
        , courses.course_image_url
        , courses.course_is_published
        , runs.year
        , runs.semester
        , instructors.instructors
        , topics.topics
        , case when courses.course_offered_by = 'OCW' then 'ocw' else 'mitx' end as offered_by
        , case courses.course_level
            when 'Undergraduate' then 'undergraduate'
            when 'Graduate' then 'graduate'
            when 'High School' then 'high_school'
            when 'Non-Credit' then 'noncredit'
            when 'Advanced' then 'advanced'
            when 'Intermediate' then 'intermediate'
            when 'Introductory' then 'introductory'
        end as level
        -- math.ceil(float(...)) in the legacy ETL
        , cast(ceil(cast(nullif(courses.course_duration_weeks, '') as double)) as integer) as weeks
        , cast(ceil(cast(nullif(courses.course_weekly_hours, '') as double)) as integer) as weekly_hours
    from courses
    inner join runs on courses.courserun_readable_id = runs.courserun_readable_id
    left join instructors on courses.courserun_readable_id = instructors.courserun_readable_id
    left join topics on courses.courserun_readable_id = topics.courserun_readable_id
)

select
    case
        when offered_by = 'ocw'
            then concat(course_number, '+', lower(coalesce(semester, '')), '_', coalesce(cast(year as varchar), ''))
        else concat('MITx+', course_number)
    end as readable_id
    , courserun_readable_id as run_id
    , course_number
    , course_title as title
    , course_url as url
    , course_description as description
    , course_image_url as image_url
    , offered_by
    , course_is_published as published
    , level
    , topics
    , instructors
    , year
    , semester
    , case when weeks is not null then concat(cast(weeks as varchar), ' weeks') end as duration
    , weeks as min_weeks
    , weeks as max_weeks
    , case when weekly_hours is not null then concat(cast(weekly_hours as varchar), ' hours/week') end
        as time_commitment
    , weekly_hours as min_weekly_hours
    , weekly_hours as max_weekly_hours
    , {{ cast_timestamp_to_iso8601(current_timestamptz()) }} as last_modified
    , 'oll' as etl_source
    , 'oll' as platform
    , 'course' as resource_type
from shaped
