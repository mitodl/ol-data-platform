with enrollment_detail as (
    select * from {{ ref('marts__combined_program_enrollment_detail') }}
)

, combined_users as (
    select * from {{ ref('marts__combined__users') }}
)

, courses_in_program as (
    select * from {{ ref('marts__combined_coursesinprogram') }}
)

, course_enrollment_detail as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
)

, courses_detail as (
    select
        courses_in_program.platform
        , courses_in_program.program_id
        , course_enrollment_detail.user_hashed_id
        , count(
            distinct case
                when course_enrollment_detail.courserunenrollment_enrollment_mode = 'verified'
                then courses_in_program.course_readable_id
            end
        ) as courses_taken_in_program
        , sum(
            case
                when upper(courses_in_program.course_title) like '%CAPSTONE%' then 1
                else 0
            end
        ) as capstone_sum
        , min(cast(substring(course_enrollment_detail.courserun_start_on, 1, 10) as date))
            as first_courserun_start_on_date
    from courses_in_program
    inner join course_enrollment_detail
        on courses_in_program.course_readable_id = course_enrollment_detail.course_readable_id
    group by
        courses_in_program.platform
        , courses_in_program.program_id
        , course_enrollment_detail.user_hashed_id
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
    , coalesce(courses_detail.courses_taken_in_program, 0) as courses_taken_in_program
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
    , date_diff(
        'day'
        , courses_detail.first_courserun_start_on_date
        , cast(substring(enrollment_detail.programcertificate_created_on, 1, 10) as date)
    ) as program_complete_days
from enrollment_detail
left join combined_users
    on enrollment_detail.platform_name = '{{ var("edxorg") }}'
    and enrollment_detail.user_id = combined_users.user_edxorg_id
left join combined_users as combined_users2
    on enrollment_detail.user_hashed_id = combined_users2.user_hashed_id
left join courses_detail
    on enrollment_detail.platform_name = courses_detail.platform
    and enrollment_detail.program_id is not distinct from courses_detail.program_id
    and enrollment_detail.user_hashed_id = courses_detail.user_hashed_id
