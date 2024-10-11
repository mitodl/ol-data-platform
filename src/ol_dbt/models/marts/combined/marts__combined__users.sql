--- This model combines intermediate users from different platforms

with mitx__users as (
    select * from {{ ref('int__mitx__users') }}
)

, non_mitx_users as (
    select * from {{ ref('int__combined__users') }}
    where platform in ('{{ var("mitxpro") }}', '{{ var("bootcamps") }}')
)

, combined_enrollments as (
    select * from {{ ref('int__combined__courserun_enrollments') }}
)

, combined_programs as (
    select * from {{ ref('marts__combined_program_enrollment_detail') }}
)

, orders as (
    select * from {{ ref('marts__combined__orders') }}
)

, income as (
    select * from {{ ref('marts__mitxonline_user_profiles') }}
)

, combined_courseruns as (
    select * from {{ ref('int__combined__course_runs') }}
)

, program_stats as (
    select
        user_email
        , count(distinct programcertificate_uuid) as cert_count
        , sum(case
            when
                program_title in (
                    'Data, Economics, and Design of Policy'
                    , 'Data, Economics, and Design of Policy: International Development'
                    , 'Data, Economics, and Design of Policy: Public Policy'
                )
                and
                programcertificate_uuid is not null
                then 1
            else 0
        end) as dedp_program_cred_count
    from combined_programs
    group by user_email
)

, orders_stats as (
    select
        user_email
        , sum(order_total_price_paid) as total_amount_paid_orders
    from orders
    group by user_email
)

, course_stats as (
    select
        combined_enrollments.user_email
        , count(distinct combined_enrollments.course_title) as num_of_course_enrolled
        , count(
            distinct
            case
                when combined_enrollments.courserungrade_is_passing = true
                    then combined_enrollments.course_title
            end
        ) as num_of_course_passed
        , min(combined_courseruns.courserun_start_on) as first_course_start_datetime
        , max(combined_courseruns.courserun_start_on) as last_course_start_datetime
        , count(distinct combined_enrollments.courseruncertificate_uuid) as number_of_courserun_certificates
    from combined_enrollments
    left join combined_courseruns
        on combined_enrollments.courserun_readable_id = combined_courseruns.courserun_readable_id
    group by combined_enrollments.user_email
)

, combined_users as (
    select
        user_hashed_id
        , user_mitxonline_id
        , user_edxorg_id
        , null as user_mitxpro_id
        , null as user_bootcamps_id
        , user_mitxonline_username
        , user_edxorg_username
        , null as user_mitxpro_username
        , null as user_bootcamps_username
        , case
            when user_is_active_on_mitxonline and user_joined_on_mitxonline > user_joined_on_edxorg
                then user_mitxonline_email
            else coalesce(user_edxorg_email, user_mitxonline_email, user_micromasters_email)
        end as user_email
        , case
            when user_is_active_on_mitxonline and user_joined_on_mitxonline > user_joined_on_edxorg
                then user_joined_on_mitxonline
            else coalesce(user_joined_on_edxorg, user_joined_on_mitxonline)
        end as user_joined_on
        , case
            when user_is_active_on_mitxonline and user_last_login_on_mitxonline > user_last_login_on_edxorg
                then user_last_login_on_mitxonline
            else coalesce(user_last_login_on_edxorg, user_last_login_on_mitxonline)
        end as user_last_login
        , case
            when user_is_active_on_mitxonline
                then user_is_active_on_mitxonline
            else user_is_active_on_edxorg
        end as user_is_active
        , case
            when is_mitxonline_user = true and is_edxorg_user = true
                then concat('{{ var("mitxonline") }}', ' and ', '{{ var("edxorg") }}')
            when is_mitxonline_user = true
                then '{{ var("mitxonline") }}'
            when is_edxorg_user = true
                then '{{ var("edxorg") }}'
        end as platforms
        , user_full_name
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
        , user_company
        , user_job_title
        , user_industry
    from mitx__users
    where is_mitxonline_user = true or is_edxorg_user = true

    union all

    select
        user_hashed_id
        , null as user_mitxonline_id
        , null as user_edxorg_id
        , user_id as user_mitxpro_id
        , null as user_bootcamps_id
        , null as user_mitxonline_username
        , null as user_edxorg_username
        , user_username as user_mitxpro_username
        , null as user_bootcamps_username
        , user_email
        , user_joined_on
        , user_last_login
        , user_is_active
        , '{{ var("mitxpro") }}' as platforms
        , user_full_name
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
        , user_company
        , user_job_title
        , user_industry
    from non_mitx_users
    where platform = '{{ var("mitxpro") }}'

    union all

    select
        user_hashed_id
        , null as user_mitxonline_id
        , null as user_edxorg_id
        , null as user_mitxpro_id
        , user_id as user_bootcamps_id
        , null as user_mitxonline_username
        , null as user_edxorg_username
        , null as user_mitxpro_username
        , user_username as user_bootcamps_username
        , user_email
        , user_joined_on
        , user_last_login
        , user_is_active
        , '{{ var("bootcamps") }}' as platforms
        , user_full_name
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
        , user_company
        , user_job_title
        , user_industry
    from non_mitx_users
    where platform = '{{ var("bootcamps") }}'
)

select
    combined_users.user_hashed_id
    , combined_users.platforms
    , combined_users.user_email
    , combined_users.user_joined_on
    , combined_users.user_last_login
    , combined_users.user_is_active
    , combined_users.user_full_name
    , combined_users.user_address_country
    , combined_users.user_highest_education
    , combined_users.user_gender
    , combined_users.user_birth_year
    , combined_users.user_company
    , combined_users.user_job_title
    , combined_users.user_industry
    , combined_users.user_mitxonline_id
    , combined_users.user_edxorg_id
    , combined_users.user_mitxpro_id
    , combined_users.user_bootcamps_id
    , combined_users.user_mitxonline_username
    , combined_users.user_edxorg_username
    , combined_users.user_mitxpro_username
    , combined_users.user_bootcamps_username
    , course_stats.num_of_course_enrolled
    , course_stats.num_of_course_passed
    , course_stats.first_course_start_datetime
    , course_stats.last_course_start_datetime
    , course_stats.number_of_courserun_certificates
    , orders_stats.total_amount_paid_orders
    , income.latest_income_usd
    , case when program_stats.cert_count > 0 then true end as has_program_certificate
    , case when program_stats.dedp_program_cred_count > 0 then true end as has_dedp_program_certificate
from combined_users
left join course_stats on combined_users.user_email = course_stats.user_email
left join program_stats on combined_users.user_email = program_stats.user_email
left join orders_stats on combined_users.user_email = orders_stats.user_email
left join income on combined_users.user_mitxonline_username = income.user_username
