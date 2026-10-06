with enrollments as (
    select * from {{ ref('tfact_enrollment') }}
    where
        platform = 'mitxonline'
        and enrollment_type = 'course'
)

-- dim_course_run and dim_course are SCD2 and can carry more than one is_current row
-- per key, so keep the latest to avoid fanning out the enrollment rows.
, d_course_run as (
    select
        courserun_pk
        , courserun_readable_id
        , courserun_title
        , course_fk
    from (
        select
            courserun_pk
            , courserun_readable_id
            , courserun_title
            , course_fk
            , row_number() over (
                partition by courserun_pk
                order by effective_date desc nulls last
            ) as _row_num
        from {{ ref('dim_course_run') }}
        where
            is_current = true
            and platform = 'mitxonline'
            -- Migrated DEDP runs hosted on edX.org are not MITx Online enrollments.
            and courserun_readable_id not like 'MITx/%'
            and courserun_readable_id not like 'course-v1:MITx+%'
    )
    where _row_num = 1
)

, d_course as (
    select
        course_pk
        , course_number
    from (
        select
            course_pk
            , course_number
            , row_number() over (
                partition by course_pk
                order by effective_date desc nulls last
            ) as _row_num
        from {{ ref('dim_course') }}
        where is_current = true
    )
    where _row_num = 1
)

, d_user as (
    select * from {{ ref('dim_user') }}
)

select
    d_course.course_number
    , d_course_run.courserun_title
    , d_course_run.courserun_readable_id
    , enrollments.enrollment_is_active as courserunenrollment_is_active
    , enrollments.enrollment_mode as courserunenrollment_enrollment_mode
    , enrollments.enrollment_status as courserunenrollment_enrollment_status
    , enrollments.enrollment_created_on as courserunenrollment_created_on
    , d_user.user_mitxonline_username as user_username
    , d_user.email as user_email
    , d_user.full_name as user_full_name
    , d_user.address_country as user_address_country
    , d_user.highest_education as user_highest_education
    , d_user.gender as user_gender
    , d_user.birth_year as user_birth_year
    , d_user.company as user_company
    , d_user.job_title as user_job_title
    , d_user.industry as user_industry
from enrollments
inner join d_course_run on enrollments.courserun_fk = d_course_run.courserun_pk
left join d_course on d_course_run.course_fk = d_course.course_pk
inner join d_user on enrollments.user_fk = d_user.user_pk
