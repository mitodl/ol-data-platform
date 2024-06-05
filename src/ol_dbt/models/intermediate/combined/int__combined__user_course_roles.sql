with mitxonline_courseroles as (
    select * from {{ ref('stg__mitxonline__openedx__mysql__user_courseaccessrole') }}
)

, mitxonline_app_users as (
    select * from {{ ref('stg__mitxonline__app__postgres__users_user') }}
)

, mitxonline_openedx_users as (
    select * from {{ ref('stg__mitxonline__openedx__mysql__auth_user') }}
)

, mitxpro_courseroles as (
    select * from {{ ref('stg__mitxpro__openedx__mysql__user_courseaccessrole') }}
)

, mitxpro_app_users as (
    select * from {{ ref('stg__mitxpro__app__postgres__users_user') }}
)

, mitxpro_openedx_users as (
    select * from {{ ref('stg__mitxpro__openedx__mysql__auth_user') }}
)

, residential_courseroles as (
    select * from {{ ref('stg__mitxresidential__openedx__user_courseaccessrole') }}
)

, residential_openedx_users as (
    select * from {{ ref('stg__mitxresidential__openedx__auth_user') }}
)

, edxorg_courseroles as (
    select * from {{ ref('stg__edxorg__s3__user_courseaccessrole') }}
)

, edxorg_users as (
    select * from {{ ref('stg__edxorg__s3__user') }}
)

, edxorg_profiles as (
    select * from {{ ref('stg__edxorg__s3__user_profile') }}
)

, combined_courseroles as (
    select
        '{{ var("mitxonline") }}' as platform
        , mitxonline_openedx_users.user_username
        , mitxonline_openedx_users.user_email
        , mitxonline_app_users.user_full_name
        , mitxonline_courseroles.courserun_readable_id
        , mitxonline_courseroles.organization
        , mitxonline_courseroles.courseaccess_role
    from mitxonline_courseroles
    inner join mitxonline_openedx_users
        on mitxonline_courseroles.openedx_user_id = mitxonline_openedx_users.openedx_user_id
    left join mitxonline_app_users
        on
            mitxonline_openedx_users.user_username = mitxonline_app_users.user_username
            or mitxonline_openedx_users.user_email = mitxonline_app_users.user_email

    union all

    select
        '{{ var("edxorg") }}' as platform
        , edxorg_users.user_username
        , edxorg_users.user_email
        , edxorg_profiles.user_full_name
        , edxorg_courseroles.courserun_edx_readable_id as courserun_readable_id
        , edxorg_courseroles.organization
        , edxorg_courseroles.courseaccess_role
    from edxorg_courseroles
    inner join edxorg_users
        on edxorg_courseroles.user_id = edxorg_users.user_id
    left join edxorg_profiles
        on edxorg_courseroles.user_id = edxorg_profiles.user_id

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , mitxpro_openedx_users.user_username
        , mitxpro_openedx_users.user_email
        , mitxpro_app_users.user_full_name
        , mitxpro_courseroles.courserun_readable_id
        , mitxpro_courseroles.organization
        , mitxpro_courseroles.courseaccess_role
    from mitxpro_courseroles
    inner join mitxpro_openedx_users
        on mitxpro_courseroles.openedx_user_id = mitxpro_openedx_users.openedx_user_id
    left join mitxpro_app_users
        on
            mitxpro_openedx_users.user_username = mitxpro_app_users.user_username
            or mitxpro_openedx_users.user_email = mitxpro_app_users.user_email

    union all

    select
        '{{ var("residential") }}' as platform
        , residential_openedx_users.user_username
        , residential_openedx_users.user_email
        , residential_openedx_users.user_full_name
        , residential_courseroles.courserun_readable_id
        , residential_courseroles.organization
        , residential_courseroles.courseaccess_role
    from residential_courseroles
    inner join residential_openedx_users on residential_courseroles.user_id = residential_openedx_users.user_id
)

select * from combined_courseroles
