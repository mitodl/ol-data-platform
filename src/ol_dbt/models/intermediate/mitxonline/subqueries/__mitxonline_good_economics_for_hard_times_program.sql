-- This query is used to determine which DEDP program to count enrollments in 14.009x for micromasters reporting
-- 14.009x can only be used to complete the requirements for one program. If the learner is enrolled in the DEDP
-- Public Policy program, then we count it for that program. Otherwise, if the learner completed the course before
-- Fall 2023 count it as a DEDP Internation Development elective

with mitxonline_programenrollments as (
    select * from {{ ref('int__mitxonline__programenrollments') }}
)


, mitxonline_courserunenrollments as (
    select * from {{ ref('int__mitxonline__courserunenrollments') }}
)

, mitxonline_courseruns as (
    select * from {{ ref('int__mitxonline__course_runs') }}
)

, query as (
    select
        mitxonline_courserunenrollments.courserunenrollment_id
        , case
            when
                count_if(
                    mitxonline_programenrollments.program_id = {{ var("dedp_mitxonline_public_policy_program_id") }}
                )
                > 0
                then {{ var("dedp_mitxonline_public_policy_program_id") }}
            when
                mitxonline_courseruns.courserun_start_on
                < to_iso8601(
                    date '{{ var("dedp_mitxonline_good_economics_for_hard_times_internation_development_cutoff") }}'
                )
                then {{ var("dedp_mitxonline_international_development_program_id") }}
        end as program_id
    from mitxonline_courserunenrollments
    inner join mitxonline_courseruns
        on mitxonline_courseruns.courserun_id = mitxonline_courserunenrollments.courserun_id
    left join mitxonline_programenrollments
        on mitxonline_programenrollments.user_id = mitxonline_courserunenrollments.user_id
    where
        mitxonline_programenrollments.program_id in (
            {{ var("dedp_mitxonline_international_development_program_id") }}
            , {{ var("dedp_mitxonline_public_policy_program_id") }}
        )
        and mitxonline_courseruns.course_number
        = '{{ var("dedp_mitxonline_good_economics_for_hard_times_course_number") }}'
    group by mitxonline_courserunenrollments.courserunenrollment_id, mitxonline_courseruns.courserun_start_on
)

select *
from query
where program_id is not null
