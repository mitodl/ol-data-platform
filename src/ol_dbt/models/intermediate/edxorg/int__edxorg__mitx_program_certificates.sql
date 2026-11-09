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

-- The date a learner finished the program: the latest of the earliest completion of each
-- of its courses, so a retake after the award does not move it. A course is its run title
-- less a trailing parenthetical, because runs of one course differ there (e.g. "(with
-- Proctored Final Exam)"), and neither the title alone nor the course key groups them
-- (Computational Thinking using Python numbers each run, 6.00.2x_5, 6.00.2x_6). The report's completed
-- date follows the course certificate's modified date, which a regeneration moves (e.g.
-- CTL.SC3x 2T2016 certificates created 2016-11-10 were all modified 2017-04-19), so the
-- certificate's created date is used when it is earlier.
, course_completions as (
    select
        program_learner_report.user_id
        , program_learner_report.program_uuid
        , lower(trim({{ regexp_replace_all('program_learner_report.course_title', "'\\s*\\([^)]*\\)\\s*$'", "''") }}))
            as course_name
        , min(
            case
                when course_certificates.courseruncertificate_created_on < program_learner_report.completed_course_on
                    then course_certificates.courseruncertificate_created_on
                else program_learner_report.completed_course_on
            end
        ) as course_completed_on
    from program_learner_report
    left join course_certificates
        on
            program_learner_report.user_id = course_certificates.user_id
            and program_learner_report.courserun_readable_id = course_certificates.courserun_readable_id
    where program_learner_report.completed_course_on is not null
    group by
        program_learner_report.user_id
        , program_learner_report.program_uuid
        , lower(trim({{ regexp_replace_all('program_learner_report.course_title', "'\\s*\\([^)]*\\)\\s*$'", "''") }}))
)

, program_completions as (
    select
        user_id
        , program_uuid
        , max(course_completed_on) as program_completed_on
    from course_completions
    group by user_id, program_uuid
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
-- that falls within 60 days after completion (the cohort actually passed), else by the
-- completion itself. An award on or after completion is kept unchanged.
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

, micromasters_programs as (
    select * from {{ ref('int__micromasters__programs') }}
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
