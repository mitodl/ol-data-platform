with program_learner_report as (
    select * from {{ ref('stg__edxorg__s3__program_learner_report') }}
)

, course_certificates as (
    select
        user_id
        , courserun_readable_id
        , min(courseruncertificate_created_on) as courseruncertificate_created_on
    from {{ ref('stg__edxorg__s3__courserun_certificate') }}
    where courseruncertificate_status = 'downloadable'
    group by user_id, courserun_readable_id
)

, completed_program_learners as (
    select
        *
        , {{ generate_hash_id(dbt.concat(["cast(user_id as varchar)", "program_uuid"])) }} as program_certificate_hashed_id
    from program_learner_report
    where user_has_completed_program = true
)

, micromasters_programs as (
    select * from {{ ref('int__micromasters__programs') }}
)

, course_name_aliases as (
    select * from {{ ref('edxorg_course_name_aliases') }}
)

-- A course is its run title less a trailing parenthetical and period, because runs of one
-- course differ there (e.g. "(with Proctored Final Exam)", and 6.00.1x's 2025 runs end
-- in "."), and neither the title alone nor the course key groups them (Computational
-- Thinking using Python numbers each run, 6.00.2x_5, 6.00.2x_6). A course renamed between
-- runs is mapped to one name by edxorg_course_name_aliases. The report's completed date
-- follows the course certificate's modified date, which a regeneration moves (e.g.
-- CTL.SC3x 2T2016 certificates created 2016-11-10 were all modified 2017-04-19), so the
-- certificate's created date is used when it is earlier.
, course_run_completions as (
    select
        program_learner_report.user_id
        , program_learner_report.program_uuid
        , lower(trim({{ regexp_replace_all('program_learner_report.course_title', "'\\s*(\\([^)]*\\))?\\s*\\.?\\s*$'", "''") }}))
            as course_name
        , case
            when course_certificates.courseruncertificate_created_on < program_learner_report.completed_course_on
                then course_certificates.courseruncertificate_created_on
            else program_learner_report.completed_course_on
        end as course_completed_on
    from program_learner_report
    left join course_certificates
        on
            program_learner_report.user_id = course_certificates.user_id
            and program_learner_report.courserun_readable_id = course_certificates.courserun_readable_id
    where program_learner_report.completed_course_on is not null
)

-- Each course's earliest completion, so a retake after the award does not move it
, course_completions as (
    select
        course_run_completions.user_id
        , course_run_completions.program_uuid
        , coalesce(course_name_aliases.course_name, course_run_completions.course_name) as course_name
        , min(course_run_completions.course_completed_on) as course_completed_on
    from course_run_completions
    left join course_name_aliases
        on course_run_completions.course_name = course_name_aliases.alias_course_name
    group by
        course_run_completions.user_id
        , course_run_completions.program_uuid
        , coalesce(course_name_aliases.course_name, course_run_completions.course_name)
)

-- MicroMasters programs record how many courses earn the certificate. XSeries programs do
-- not, so they take the latest course completion.
, program_required_courses as (
    select
        program_learner_report.program_uuid
        , max(micromasters_programs.program_num_required_courses) as required_course_count
    from program_learner_report
    inner join micromasters_programs
        on (
            program_learner_report.program_title like {{ dbt.concat(["micromasters_programs.program_title", "'%'"]) }}
            or program_learner_report.program_title like {{ dbt.concat(["'%'", "micromasters_programs.program_title"]) }}
        )
    group by program_learner_report.program_uuid
)

, numbered_course_completions as (
    select
        *
        , row_number() over (
            partition by user_id, program_uuid
            order by course_completed_on, course_name
        ) as course_completion_number
    from course_completions
)

-- The date a learner finished the program: when they completed the number of courses it
-- requires, so a course taken beyond that (e.g. an extra elective) does not move it. A
-- learner who has completed fewer, or a program with no recorded count, takes the latest
-- course completion.
, program_completions as (
    select
        numbered_course_completions.user_id
        , numbered_course_completions.program_uuid
        , coalesce(
            max(
                case
                    when
                        numbered_course_completions.course_completion_number
                        = program_required_courses.required_course_count
                        then numbered_course_completions.course_completed_on
                end
            )
            , max(numbered_course_completions.course_completed_on)
        ) as program_completed_on
    from numbered_course_completions
    left join program_required_courses
        on numbered_course_completions.program_uuid = program_required_courses.program_uuid
    group by numbered_course_completions.user_id, numbered_course_completions.program_uuid
)

, program_awards as (
    select
        user_id
        , program_uuid
        -- program_certificate_awarded_on is each run's earliest award, and the latest of
        -- those per program is what this model has always reported.
        , max(program_certificate_awarded_on) as earliest_awarded_on
        , max(program_certificate_last_awarded_on) as latest_awarded_on
    from completed_program_learners
    group by user_id, program_uuid
)

-- edX.org's reported award date is wrong in two ways, both measured on 2026-11-09:
-- old-format reports in some programs carry the final run's scheduled certificate date
-- (run end + 48 h, e.g. Computational Thinking using Python, Supply-Chain Management),
-- days before the final course certificate existed; and Supply Chain Management carries
-- the cohort date of a failed final-exam attempt until the learner passes. So an award
-- more than a day before the program completion is replaced: by the latest award when
-- that falls between a day before and 60 days after completion (the cohort actually
-- passed), else by the completion itself. Both checks allow the same day of slack, so an
-- award no more than a day before completion is kept unchanged.
, program_certificate_dates as (
    select
        program_awards.user_id
        , program_awards.program_uuid
        , case
            when
                program_awards.earliest_awarded_on is null
                or program_completions.program_completed_on is null
                or {{ date_diff("'second'", from_iso8601_timestamp("program_awards.earliest_awarded_on"), from_iso8601_timestamp("program_completions.program_completed_on")) }}
                <= 86400
                then program_awards.earliest_awarded_on
            when
                {{ date_diff("'second'", from_iso8601_timestamp("program_completions.program_completed_on"), from_iso8601_timestamp("program_awards.latest_awarded_on")) }}
                between -86400 and 5184000
                then program_awards.latest_awarded_on
            else program_completions.program_completed_on
        end as program_certificate_awarded_on
    from program_awards
    left join program_completions
        on
            program_awards.user_id = program_completions.user_id
            and program_awards.program_uuid = program_completions.program_uuid
)

, completed_program_learners_sorted as (
    select
        *
        , row_number() over (
            partition by user_id, program_uuid
            order by program_certificate_awarded_on desc
        ) as row_num
    from completed_program_learners
)

select
    completed_program_learners_sorted.program_certificate_hashed_id
    , completed_program_learners_sorted.program_type
    , completed_program_learners_sorted.program_uuid
    , completed_program_learners_sorted.program_title
    , completed_program_learners_sorted.user_id
    , completed_program_learners_sorted.user_username
    , completed_program_learners_sorted.user_full_name
    , completed_program_learners_sorted.user_has_completed_program
    , program_certificate_dates.program_certificate_awarded_on
    , micromasters_programs.program_id as micromasters_program_id
from completed_program_learners_sorted
inner join program_certificate_dates
    on
        completed_program_learners_sorted.user_id = program_certificate_dates.user_id
        and completed_program_learners_sorted.program_uuid = program_certificate_dates.program_uuid
left join micromasters_programs
--- Finance is split into MIT Finance and Finance,
--- Statistics and Data Science is split into Statistics and Data Science (General track)
    -- and Statistics and Data Science
    on (
        completed_program_learners_sorted.program_title like {{ dbt.concat(["micromasters_programs.program_title", "'%'"]) }}
        or completed_program_learners_sorted.program_title like {{ dbt.concat(["'%'", "micromasters_programs.program_title"]) }}
    )
where completed_program_learners_sorted.row_num = 1
