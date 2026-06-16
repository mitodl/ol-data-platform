-- Migrated to dim_user for user identity.
-- user_last_login, user_street_address, user_address_state_or_territory, and
-- user_address_postal_code are not yet in dim_user; they remain sourced from
-- int__combined__users until dim_user is extended to include those fields.

with users as (
    select * from {{ ref('dim_user') }}
)

-- Supplemental: address and last-login fields not yet available in dim_user.
-- Deduped by email with max() to handle users appearing on multiple platforms.
, users_supplement as (
    select
        lower(user_email) as user_email_lower
        , max(user_last_login) as user_last_login
        , max(user_street_address) as user_street_address
        , max(user_address_city) as user_address_city
        , max(user_address_state_or_territory) as user_address_state_or_territory
        , max(user_address_postal_code) as user_address_postal_code
    from {{ ref('int__combined__users') }}
    group by lower(user_email)
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

select
    -- user_hashed_id: replicates generate_hash_id logic from the former
    -- int__mitx__users and int__combined__users to preserve cross-model join
    -- compatibility (e.g. learner_demographics_and_cert_info, cheating_detection_report).
    case
        when users.mitxonline_application_user_id is not null
            then {{ generate_hash_id("cast(users.mitxonline_application_user_id as varchar) || '" ~ var('mitxonline') ~ "'") }}
        when users.edxorg_openedx_user_id is not null
            then {{ generate_hash_id("cast(users.edxorg_openedx_user_id as varchar) || '" ~ var('edxorg') ~ "'") }}
        when users.micromasters_user_id is not null
            then {{ generate_hash_id("cast(users.micromasters_user_id as varchar) || '" ~ var('micromasters') ~ "'") }}
        when users.mitxpro_application_user_id is not null
            then {{ generate_hash_id("cast(users.mitxpro_application_user_id as varchar) || '" ~ var('mitxpro') ~ "'") }}
        when users.bootcamps_application_user_id is not null
            then {{ generate_hash_id("cast(users.bootcamps_application_user_id as varchar) || '" ~ var('bootcamps') ~ "'") }}
        when users.emeritus_user_id is not null
            then {{ generate_hash_id("cast(users.emeritus_user_id as varchar) || '" ~ var('emeritus') ~ "'") }}
        when users.global_alumni_user_id is not null
            then {{ generate_hash_id("cast(users.global_alumni_user_id as varchar) || '" ~ var('global_alumni') ~ "'") }}
    end as user_hashed_id
    -- Derived: platform label
    , case
        when users.mitxonline_application_user_id is not null and users.edxorg_openedx_user_id is not null
            then concat('{{ var("mitxonline") }}', ' and ', '{{ var("edxorg") }}')
        when users.mitxonline_application_user_id is not null
            then '{{ var("mitxonline") }}'
        when users.edxorg_openedx_user_id is not null
            then '{{ var("edxorg") }}'
        when users.mitxpro_application_user_id is not null
            then '{{ var("mitxpro") }}'
        when users.bootcamps_application_user_id is not null
            then '{{ var("bootcamps") }}'
        when users.emeritus_user_id is not null
            then '{{ var("emeritus") }}'
        when users.global_alumni_user_id is not null
            then '{{ var("global_alumni") }}'
    end as platforms
    , users.email as user_email
    -- Derived: joined_on — prefer MITx Online when active and newer than edX.org
    , case
        when
            users.user_is_active_on_mitxonline
            and users.user_joined_on_mitxonline > users.user_joined_on_edxorg
            then users.user_joined_on_mitxonline
        else coalesce(
            users.user_joined_on_edxorg
            , users.user_joined_on_mitxonline
            , users.user_joined_on_mitxpro
            , users.user_joined_on_bootcamps
        )
    end as user_joined_on
    -- Not yet in dim_user; sourced from int__combined__users supplement
    , users_supplement.user_last_login
    -- Derived: is_active — prefer MITx Online, fallback across platforms
    , coalesce(
        users.user_is_active_on_mitxonline
        , users.user_is_active_on_edxorg
        , users.user_is_active_on_mitxpro
        , users.user_is_active_on_bootcamps
    ) as user_is_active
    , users.full_name as user_full_name
    , users.address_country as user_address_country
    , users.highest_education as user_highest_education
    , users.gender as user_gender
    , users.birth_year as user_birth_year
    , users.company as user_company
    , users.job_title as user_job_title
    , users.industry as user_industry
    -- Not yet in dim_user; sourced from int__combined__users supplement
    , users_supplement.user_street_address
    , users_supplement.user_address_city
    , users_supplement.user_address_state_or_territory
    , users_supplement.user_address_postal_code
    -- Platform IDs
    , users.mitxonline_application_user_id as user_mitxonline_id
    , users.edxorg_openedx_user_id as user_edxorg_id
    , users.mitxpro_application_user_id as user_mitxpro_id
    , users.bootcamps_application_user_id as user_bootcamps_id
    -- Usernames
    , users.user_mitxonline_username
    , users.user_edxorg_username
    , users.user_mitxpro_username
    -- Not yet in dim_user
    , null as user_bootcamps_username
    -- Stats (upstream sources being migrated by other epic tickets)
    , course_stats.num_of_course_enrolled
    , course_stats.num_of_course_passed
    , course_stats.first_course_start_datetime
    , course_stats.last_course_start_datetime
    , course_stats.number_of_courserun_certificates
    , orders_stats.total_amount_paid_orders
    , income.latest_income_usd
    , case when program_stats.cert_count > 0 then true end as has_program_certificate
    , case when program_stats.dedp_program_cred_count > 0 then true end as has_dedp_program_certificate
    -- Legacy hash for transition: ~50k users who were 'edX.org'-only in the previous
    -- mart have been linked to a MITx Online account in dim_user, causing user_hashed_id
    -- to change (edxorg_id-based → mitxonline_id-based). This column preserves the
    -- old edX.org hash so that downstream consumers (Hightouch, cross-model joins) can
    -- perform a graceful key migration without a hard cutover.
    -- Track removal in: https://github.com/mitodl/ol-data-platform/issues/TODO
    -- Remove once all consumers have migrated to user_hashed_id.
    , case
        when users.edxorg_openedx_user_id is not null
            then {{ generate_hash_id("cast(users.edxorg_openedx_user_id as varchar) || '" ~ var('edxorg') ~ "'") }}
    end as user_hashed_id_edxorg_legacy
from users
left join users_supplement on users.email = users_supplement.user_email_lower
left join course_stats on users.email = lower(course_stats.user_email)
left join program_stats on users.email = lower(program_stats.user_email)
left join orders_stats on users.email = lower(orders_stats.user_email)
left join income on users.user_mitxonline_username = income.user_username
-- Exclude MicroMasters-only users: the original mart filtered to
-- (is_mitxonline_user OR is_edxorg_user) for the MITx branch, and MITxPro,
-- Bootcamps, Emeritus, Global Alumni for the other branches. dim_user also
-- includes pure MicroMasters-only users (no other platform), which the
-- original mart never emitted. Filter them out to preserve row parity.
where (
    users.mitxonline_application_user_id is not null
    or users.edxorg_openedx_user_id is not null
    or users.mitxpro_application_user_id is not null
    or users.bootcamps_application_user_id is not null
    or users.emeritus_user_id is not null
    or users.global_alumni_user_id is not null
)
