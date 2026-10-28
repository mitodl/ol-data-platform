with enrollment_detail as (
    select * from {{ ref('marts__combined_program_enrollment_detail') }}
)

, combined_users as (
    select * from {{ ref('marts__combined__users') }}
)

, course as (
    select * from {{ ref('dim_course') }}
)

, course_enrollment_detail as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
)

, program_course as (
    select * from {{ ref('bridge_program_course') }}
)

, program as (
    select * from {{ ref('dim_program') }}
)

-- An enrollment row with a readable id matches the programs that carry it, and one
-- without (an edX.org XSeries row) matches every program with its title, e.g. both the
-- edX.org program and the MITx Online program it moved to. Each key's courses are
-- counted once across all of its programs, so a row matches one courses_detail row.
, program_course_match_keys as (
    select
        program_course.course_fk
        , 'readable_id' as match_type
        , program.program_readable_id as match_value
    from program_course
    inner join program
        on program_course.program_fk = program.program_pk
    where program.program_readable_id is not null

    union all

    select
        program_course.course_fk
        , 'title' as match_type
        , program.program_title as match_value
    from program_course
    inner join program
        on program_course.program_fk = program.program_pk
)

, courses_detail as (
    select
        course_enrollment_detail.platform
        , program_course_match_keys.match_type
        , program_course_match_keys.match_value
        , course_enrollment_detail.user_email
        , count(
            distinct case
                when course_enrollment_detail.courserunenrollment_enrollment_mode = 'verified'
                then course.course_readable_id
            end
        ) as verified_courses_taken_in_program
        , count(
            distinct case
                when
                    course_enrollment_detail.courserunenrollment_enrollment_mode = 'verified'
                    and course_enrollment_detail.courserungrade_is_passing = true
                then course.course_readable_id
            end
        ) as verified_courses_passed_in_program
        , count(
            distinct case
                when course_enrollment_detail.courserungrade_is_passing = true
                then course.course_readable_id
            end
        ) as all_courses_passed_in_program
        , sum(
            case
                when upper(course.course_title) like '%CAPSTONE%'
                and course_enrollment_detail.courserunenrollment_enrollment_mode = 'verified'
                then 1
                else 0
            end
        ) as capstone_sum
        , count(distinct(
            case
                when course_enrollment_detail.courseruncertificate_is_earned = true
                then course.course_readable_id
                else null
            end
        )) as course_certificate_sum
    from course_enrollment_detail
        inner join course
            on course_enrollment_detail.course_readable_id = course.course_readable_id
            and course.is_current = true
        inner join (select distinct course_fk, match_type, match_value from program_course_match_keys)
            as program_course_match_keys
            on course.course_pk = program_course_match_keys.course_fk
    group by
        course_enrollment_detail.platform
        , program_course_match_keys.match_type
        , program_course_match_keys.match_value
        , course_enrollment_detail.user_email
)

select
    enrollment_detail.platform_name
    , enrollment_detail.program_id
    , enrollment_detail.program_title
    , enrollment_detail.program_name
    , enrollment_detail.program_track
    , enrollment_detail.program_type
    , enrollment_detail.program_is_live
    , enrollment_detail.program_readable_id
    , enrollment_detail.user_hashed_id
    , enrollment_detail.user_id
    , enrollment_detail.user_email
    , enrollment_detail.user_username
    , enrollment_detail.user_full_name
    , enrollment_detail.user_has_completed_program
    , enrollment_detail.programenrollment_is_active
    , enrollment_detail.programenrollment_created_on
    , enrollment_detail.programenrollment_enrollment_status
    , enrollment_detail.programenrollment_enrollment_mode
    , enrollment_detail.programcertificate_created_on
    , enrollment_detail.programcertificate_is_revoked
    , enrollment_detail.programcertificate_uuid
    , enrollment_detail.programcertificate_url
    , coalesce(
        combined_users.user_highest_education, combined_users2.user_highest_education
    ) as user_highest_education
    , coalesce(combined_users.user_industry, combined_users2.user_industry) as user_industry
    , coalesce(combined_users.user_company, combined_users2.user_company) as user_company
    , coalesce(combined_users.user_job_title, combined_users2.user_job_title) as user_job_title
    , coalesce(combined_users.user_birth_year, combined_users2.user_birth_year) as user_birth_year
    , coalesce(combined_users.user_gender, combined_users2.user_gender) as user_gender
    , coalesce(
        combined_users.user_address_country, combined_users2.user_address_country
    ) as user_address_country
    , coalesce(courses_detail.verified_courses_taken_in_program, 0) as verified_courses_taken_in_program
    , coalesce(courses_detail.course_certificate_sum, 0) as course_certificate_sum
    , coalesce(courses_detail.verified_courses_taken_in_program, 0) as courses_taken_in_program
    , coalesce(courses_detail.verified_courses_passed_in_program, 0) as verified_courses_passed_in_program
    , coalesce(courses_detail.all_courses_passed_in_program, 0) as all_courses_passed_in_program
    , coalesce(
        combined_users.user_street_address, combined_users2.user_street_address
    ) as user_street_address
    , coalesce(
        combined_users.user_address_city, combined_users2.user_address_city
    ) as user_address_city
    , coalesce(
        combined_users.user_address_state_or_territory
        , combined_users2.user_address_state_or_territory
    ) as user_address_state_or_territory
    , coalesce(
        combined_users.user_address_postal_code, combined_users2.user_address_postal_code
    ) as user_address_postal_code
    , case when courses_detail.capstone_sum > 0 then 'Y' else 'N' end as capstone_ind
    , enrollment_detail.program_completion_days as program_complete_days
from enrollment_detail
left join combined_users
    on enrollment_detail.platform_name = '{{ var("edxorg") }}'
    and enrollment_detail.user_id = combined_users.user_edxorg_id
left join combined_users as combined_users2
    on enrollment_detail.user_hashed_id = combined_users2.user_hashed_id
left join courses_detail
    on enrollment_detail.platform_name = courses_detail.platform
    and courses_detail.match_type = if(enrollment_detail.program_readable_id is not null, 'readable_id', 'title')
    and coalesce(enrollment_detail.program_readable_id, enrollment_detail.program_title) = courses_detail.match_value
    and enrollment_detail.user_email = courses_detail.user_email
