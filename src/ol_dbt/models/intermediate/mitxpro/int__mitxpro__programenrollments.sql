-- Enrollment information for MITxPro
with
    enrollments as (select * from {{ ref("stg__mitxpro__app__postgres__courses_programenrollment") }}),
    programs as (select program_id, program_title from {{ ref("stg__mitxpro__app__postgres__courses_program") }}),
    users as (select user_id, user_username, user_email from {{ ref("stg__mitxpro__app__postgres__users_user") }}),
    mitxpro_enrollments as (
        select
            enrollments.programenrollment_id,
            enrollments.programenrollment_is_active,
            enrollments.user_id,
            enrollments.program_id,
            enrollments.programenrollment_created_on,
            enrollments.programenrollment_enrollment_status,
            programs.program_title,
            users.user_username,
            users.user_email,
            enrollments.ecommerce_company_id,
            enrollments.ecommerce_order_id
        from enrollments
        left join programs on enrollments.program_id = programs.program_id
        left join users on enrollments.user_id = users.user_id
    )

select *
from mitxpro_enrollments
