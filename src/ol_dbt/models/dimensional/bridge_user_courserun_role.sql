{{ config(
    materialized='table'
) }}

-- User to course run roles bridge across all platforms.
-- Grain: one row per (user, course_run, role).
with user_course_roles as (
    select
        user_email
        , courserun_readable_id
        , platform_code
        , courseaccess_role
    from {{ ref('int__combined__user_course_roles') }}
)

, dim_user as (
    select user_pk, email
    from {{ ref('dim_user') }}
)

, dim_course_run as (
    select courserun_pk, courserun_readable_id, platform
    from {{ ref('dim_course_run') }}
    where is_current = true
)

select
    du.user_pk as user_fk
    , cr.courserun_pk as courserun_fk
    -- Some course runs have no courserun_pk (due to missing course run metadata in source), so retaining readable_id allows
     -- those course runs to still be identifiable in downstream reporting tables
    , ucr.courserun_readable_id
    , ucr.courseaccess_role
from user_course_roles as ucr
inner join dim_user as du on lower(ucr.user_email) = lower(du.email)
left join dim_course_run as cr
    on
        ucr.courserun_readable_id = cr.courserun_readable_id
        and ucr.platform_code = cr.platform
