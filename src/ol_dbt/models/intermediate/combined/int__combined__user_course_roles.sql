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

, user_course_role_seed_file as (
    select * from {{ ref('user_course_roles') }}
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
        , {{ generate_hash_id('mitxonline_openedx_users.user_email') }} as hashed_user_email
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
        , {{ generate_hash_id('edxorg_users.user_email') }} as hashed_user_email
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
        , {{ generate_hash_id('mitxpro_openedx_users.user_email') }} as hashed_user_email
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
        , {{ generate_hash_id('residential_openedx_users.user_email') }} as hashed_user_email
    from residential_courseroles
    inner join residential_openedx_users on residential_courseroles.user_id = residential_openedx_users.user_id
)

, combined_users as (
    select * from (
        select
            platform
            , user_username
            , user_email
            , hashed_user_email
            , user_full_name
            , row_number() over (partition by hashed_user_email, platform order by user_email desc) as row_num
        from combined_courseroles
    )
    where row_num = 1
)

, combined_courseroles_with_seed as (
    select
        combined_courseroles.user_username
        , combined_courseroles.user_email
        , combined_courseroles.user_full_name
        , coalesce(combined_courseroles.courserun_readable_id, user_course_role_seed_file.courserun_readable_id)
        as courserun_readable_id
        , coalesce(combined_courseroles.organization, user_course_role_seed_file.organization)
        as organization
        , coalesce(combined_courseroles.courseaccess_role, user_course_role_seed_file.courseaccess_role)
        as courseaccess_role
        , coalesce(combined_courseroles.hashed_user_email, user_course_role_seed_file.hashed_user_email)
        as hashed_user_email
        , coalesce(combined_courseroles.platform, user_course_role_seed_file.platform) as platform
    from combined_courseroles
    full outer join user_course_role_seed_file
        on
            combined_courseroles.platform = user_course_role_seed_file.platform
            and combined_courseroles.hashed_user_email = user_course_role_seed_file.hashed_user_email
            and combined_courseroles.courserun_readable_id = user_course_role_seed_file.courserun_readable_id
            and combined_courseroles.courseaccess_role = user_course_role_seed_file.courseaccess_role
)

--- augment user's email, full name added by seed file
select
    combined_courseroles_with_seed.platform
    , combined_users.user_username
    , combined_users.user_email
    , combined_users.user_full_name
    , combined_courseroles_with_seed.courserun_readable_id
    , combined_courseroles_with_seed.organization
    , combined_courseroles_with_seed.courseaccess_role
    , combined_courseroles_with_seed.hashed_user_email
from combined_courseroles_with_seed
left join combined_users
    on
        combined_courseroles_with_seed.platform = combined_users.platform
        and combined_courseroles_with_seed.hashed_user_email = combined_users.hashed_user_email
