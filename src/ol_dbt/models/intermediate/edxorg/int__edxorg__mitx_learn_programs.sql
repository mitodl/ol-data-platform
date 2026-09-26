{#
  The edX.org programs MIT Learn lists, one row per program, with the fields MIT
  Learn's legacy mit_edx programs ETL derived from the programs API (see
  learning_resources/etl/mit_edx_programs.py and openedx.py there). A program is
  listed when it is in the latest extraction, authored by MITx or MITx_PRO, active,
  not a MicroMasters program, and not titled as deleted.

  Dates, price, pace and availability come from the program courses' runs as the
  programs API reports them (int__edxorg__mitx_learn_program_course_runs):
  - dates span the published runs of courses not excluded from search;
  - price sums, per such course, the cheapest paid seat of its earliest-starting
    published run;
  - pace is every pacing type among each course's published runs, or self-paced for
    a course with none;
  - availability is dated if any course has a dated published run, anytime if every
    course's published runs are all available anytime, and unknown otherwise.
  Evaluated when the model is built.
#}

with programs as (
    select *
    from {{ ref('stg__edxorg__s3__programs') }}
    where program_retrieved_at = (select max(program_retrieved_at) from {{ ref('stg__edxorg__s3__programs') }})
)

, program_courses as (
    {{ edxorg_current_program_courses() }}
)

, runs as (
    select * from {{ ref('int__edxorg__mitx_learn_program_course_runs') }}
)

, listed_programs as (
    select *
    from programs
    where
        program_status = 'active'
        and lower(program_type) not like '%micromasters%'
        and (
            '|' || replace(program_organization, ', ', '|') || '|' like '%|MITx|%'
            or '|' || replace(program_organization, ', ', '|') || '|' like '%|MITx\_PRO|%' escape '\'
        )
        and not (
            lower(trim(program_title)) like '%[delete]%'
            or lower(trim(program_title)) like '%(delete)%'
            or lower(trim(program_title)) like '%delete %'
            or lower(trim(program_title)) = 'delete'
        )
)

, searchable_published_runs as (
    select runs.*
    from runs
    inner join program_courses
        on runs.program_uuid = program_courses.program_uuid
        and runs.course_key = program_courses.course_key
    where runs.run_is_published and not program_courses.course_is_excluded_from_search
)

, program_dates as (
    select
        program_uuid
        , min(run_start_on) as start_date
        , max(run_end_on) as end_date
        , min(run_enrollment_start_on) as enrollment_start
        , max(run_enrollment_end_on) as enrollment_end
    from searchable_published_runs
    group by program_uuid
)

, course_prices as (
    select
        program_uuid
        , course_key
        , coalesce(run_min_paid_price, 0) as course_price
        , coalesce(run_first_seat_currency, 'USD') as course_currency
    from (
        select
            *
            , row_number() over (
                partition by program_uuid, course_key order by run_start_on asc, run_key
            ) as run_rank
        from searchable_published_runs
    )
    where run_rank = 1
)

, program_prices as (
    select
        program_courses.program_uuid
        , sum(coalesce(course_prices.course_price, 0)) as price
        -- the first course's currency, or USD when it has no priced run
        , min_by(coalesce(course_prices.course_currency, 'USD'), program_courses.course_position) as currency
    from program_courses
    left join course_prices
        on program_courses.program_uuid = course_prices.program_uuid
        and program_courses.course_key = course_prices.course_key
    group by program_courses.program_uuid
)

, run_availability as (
    select
        program_uuid
        , course_key
        , case
            when run_availability = 'Archived' then 'anytime'
            when run_pacing_type = 'self_paced'
                and run_start_on is not null
                and {{ from_iso8601_timestamp('run_start_on') }} < current_timestamp
                then 'anytime'
            else 'dated'
        end as availability
        , run_pacing_type
    from runs
    where run_is_published
)

, course_attributes as (
    select
        program_courses.program_uuid
        , program_courses.course_key
        , case
            when bool_or(run_availability.availability = 'dated') then 'dated'
            when count(run_availability.availability) > 0 then 'anytime'
        end as course_availability
    from program_courses
    left join run_availability
        on program_courses.program_uuid = run_availability.program_uuid
        and program_courses.course_key = run_availability.course_key
    group by program_courses.program_uuid, program_courses.course_key
)

, program_availability as (
    select
        program_uuid
        , case
            when bool_or(course_availability = 'dated') then 'dated'
            when bool_and(coalesce(course_availability = 'anytime', false)) then 'anytime'
        end as availability
    from course_attributes
    group by program_uuid
)

, course_paces as (
    select distinct
        program_courses.program_uuid
        , coalesce(run_availability.run_pacing_type, 'self_paced') as pace
    from program_courses
    left join run_availability
        on program_courses.program_uuid = run_availability.program_uuid
        and program_courses.course_key = run_availability.course_key
)

, program_paces as (
    select
        program_uuid
        , array_agg(pace order by pace) as pace
    from course_paces
    group by program_uuid
)

, program_course_ids as (
    select
        program_uuid
        , array_agg(course_key order by course_position) as course_readable_ids
    from program_courses
    group by program_uuid
)

select
    listed_programs.program_uuid as readable_id
    , listed_programs.program_title as title
    , listed_programs.program_subtitle as description
    , listed_programs.program_marketing_url as url
    , listed_programs.program_banner_image_url as image_url
    , listed_programs.program_updated_on as last_modified
    -- MIT Learn keeps the level override only when it is one of its level labels
    , case listed_programs.program_level_type_override
        when 'Undergraduate' then 'undergraduate'
        when 'Graduate' then 'graduate'
        when 'High School' then 'high_school'
        when 'Non-Credit' then 'noncredit'
        when 'Advanced' then 'advanced'
        when 'Intermediate' then 'intermediate'
        when 'Introductory' then 'introductory'
    end as level
    , program_dates.start_date
    , program_dates.end_date
    , program_dates.enrollment_start
    , program_dates.enrollment_end
    , coalesce(program_prices.price, 0) as price
    , coalesce(program_prices.currency, 'USD') as currency
    , program_paces.pace
    -- a program with no courses is available anytime, as all() of nothing is true
    , coalesce(program_availability.availability, case when program_course_ids.program_uuid is null then 'anytime' end)
        as availability
    , program_course_ids.course_readable_ids
    , listed_programs.program_retrieved_at as retrieved_at
from listed_programs
left join program_dates on listed_programs.program_uuid = program_dates.program_uuid
left join program_prices on listed_programs.program_uuid = program_prices.program_uuid
left join program_paces on listed_programs.program_uuid = program_paces.program_uuid
left join program_availability on listed_programs.program_uuid = program_availability.program_uuid
left join program_course_ids on listed_programs.program_uuid = program_course_ids.program_uuid
