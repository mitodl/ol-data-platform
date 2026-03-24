with edx_certificate as (
    select
        *
        ,  {{ format_course_id('courserun_readable_id', false) }} as courseware_id
    from {{ ref('int__edxorg__mitx_courserun_certificates') }}
    where user_email not like 'retired__user%' and user_username not like 'retired__user%'
)

, program_entitlements as (
    select distinct
        user_email
        , user_id as user_edxorg_id
        , user_full_name
    from {{ ref('stg__edxorg__program_entitlement') }}
)

, retired_users as (
    select user_id
    from {{ ref('int__edxorg__mitx_users') }}
    where
        user_is_active = false
)

, mitx_user as (
    select * from {{ ref('int__mitx__users') }}
)

, edx_certificate_user as (
   select
        edx_certificate.*
        , mitx_user.user_gender
        , mitx_user.user_birth_year
        , mitx_user.user_address_country
        , row_number() over (
              partition by edx_certificate.user_email order by mitx_user.user_joined_on_edxorg desc
        ) as row_number
    from edx_certificate
    inner join mitx_user on edx_certificate.user_email = mitx_user.user_edxorg_email
)

, cert_users as (
    select
        lower(edx_certificate_user.user_email) as user_email
        , edx_certificate_user.user_full_name
        , edx_certificate_user.user_gender
        , edx_certificate_user.user_birth_year
        , edx_certificate_user.user_address_country as user_country
    from edx_certificate_user
    left join mitx_user as mitx_user1 on edx_certificate_user.user_mitxonline_username = mitx_user1.user_mitxonline_username
    left join mitx_user as mitx_user2 on lower(edx_certificate_user.user_email) = lower(mitx_user2.user_mitxonline_email)
    left join retired_users on edx_certificate_user.user_id = retired_users.user_id
    where
        edx_certificate_user.row_number = 1
        and mitx_user1.user_mitxonline_username is null
        and mitx_user2.user_mitxonline_email is null
        and retired_users.user_id is null
)

, entitlement_users as (
    select distinct
        lower(program_entitlements.user_email) as user_email
        , coalesce(mitx_user.user_full_name, program_entitlements.user_full_name, '') as user_full_name
        , mitx_user.user_gender
        , mitx_user.user_birth_year
        , mitx_user.user_address_country as user_country
    from program_entitlements
    left join mitx_user
         on program_entitlements.user_edxorg_id = mitx_user.user_edxorg_id
    left join mitx_user as mitx_user2
        on lower(program_entitlements.user_email) = lower(mitx_user2.user_mitxonline_email)
    where
        mitx_user.user_mitxonline_id is null
        and mitx_user2.user_mitxonline_email is null
)

select
    coalesce(cert_users.user_email, entitlement_users.user_email) as user_email
    , coalesce(cert_users.user_full_name, entitlement_users.user_full_name) as user_full_name
    , coalesce(cert_users.user_gender, entitlement_users.user_gender) as user_gender
    , coalesce(cert_users.user_birth_year, entitlement_users.user_birth_year) as user_birth_year
    , coalesce(cert_users.user_country, entitlement_users.user_country) as user_country
from cert_users
full outer join entitlement_users
    on cert_users.user_email = entitlement_users.user_email
