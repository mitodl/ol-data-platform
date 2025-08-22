-- Enrollment information for MITx Online

with enrollments as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_programenrollment') }}
)

, programs as (
    select
        program_id
        , program_title
    from {{ ref('stg__mitxonline__app__postgres__courses_program') }}
)

, users as (
    select
        user_id
        , user_username
        , user_email
    from {{ ref('stg__mitxonline__app__postgres__users_user') }}
)

, mitxonline_enrollments as (
    select
        enrollments.programenrollment_id
        , enrollments.programenrollment_is_active
        , enrollments.user_id
        , enrollments.program_id
        , enrollments.programenrollment_created_on
        , enrollments.programenrollment_enrollment_mode
        , enrollments.programenrollment_enrollment_status
        , programs.program_title
        , users.user_username
        , users.user_email
    from enrollments
    inner join programs on enrollments.program_id = programs.program_id
    inner join users on enrollments.user_id = users.user_id
)

select * from mitxonline_enrollments
