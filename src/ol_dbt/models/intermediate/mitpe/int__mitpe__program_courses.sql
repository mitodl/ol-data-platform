{#
  The courses in each MIT Professional Education program. The feed lists a program's
  courses by title, so membership is resolved by matching those titles to course
  titles after the same trimming and unescaping; unmatched titles are dropped.
  Course titles are unique (tested on int__mitpe__learning_resources' courses), so a
  title identifies one course.
#}

with resources as (
    select * from {{ ref('int__mitpe__learning_resources') }}
)

, programs as (
    select
        stg.course_uuid as program_readable_id
        , split(stg.program_course_titles_raw, '|') as course_titles
    from {{ ref('stg__mitpe__api__courses') }} as stg
    inner join resources on stg.course_uuid = resources.readable_id
    where
        resources.resource_type = 'program'
        and stg.program_course_titles_raw is not null
        and stg.program_course_titles_raw != ''
)

, program_titles as (
    select
        programs.program_readable_id
        , title_index.course_position
        , {{ element_at_array('programs.course_titles', 'title_index.course_position') }} as course_title
    from programs
    cross join {{ unnest_sequence(array_length('programs.course_titles'), 'title_index', 'course_position') }}
)

select
    program_titles.program_readable_id
    , program_titles.course_position
    , courses.readable_id as course_readable_id
from program_titles
inner join resources as courses
    on {{ html_unescape(regexp_replace_all('program_titles.course_title', "'^\\s+|\\s+$'", "''")) }} = courses.title
    and courses.resource_type = 'course'
where program_titles.course_title != ''
