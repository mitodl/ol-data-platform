-- The facts can hold several rows per user and run (re-enrollments, merged accounts,
-- regrades, reissued certificates); each CTE keeps one per platform
with enrollments as (
    select
        platform
        , user_fk
        , courserun_fk
        , enrollment_is_active
        , enrollment_created_on
        , enrollment_mode
        , row_number() over (
            partition by platform, user_fk, courserun_fk
            order by enrollment_is_active desc nulls last, enrollment_created_on desc nulls last
        ) as row_num
    from {{ ref('tfact_enrollment') }}
    where platform in ('mitxpro', 'mitxonline') and enrollment_type = 'course'
)

, grades as (
    select
        platform
        , user_fk
        , courserun_fk
        , grade_value
        , letter_grade
        , is_passing
        , {{ from_iso8601_timestamp('coalesce(grade_updated_on, grade_created_on)') }} as grade_changed_on
        , row_number() over (
            partition by platform, user_fk, courserun_fk
            order by grade_updated_on desc nulls last, grade_created_on desc nulls last, grade_id desc
        ) as row_num
    from {{ ref('tfact_grade') }}
    where platform in ('mitxpro', 'mitxonline')
)

-- A valid certificate ranks first, so a run reads as revoked only when no valid one is left
, certificates as (
    select
        platform
        , user_fk
        , courserun_fk
        , certificate_uuid
        , certificate_is_revoked
        , certificate_issued_on
        , row_number() over (
            partition by platform, user_fk, courserun_fk
            order by certificate_is_revoked asc nulls last, certificate_issued_on desc nulls last, certificate_id desc
        ) as row_num
    from {{ ref('tfact_certificate') }}
    where platform in ('mitxpro', 'mitxonline') and certificate_scope = 'course' and is_current = true
)

, xpro_compared as (
    select
        dim_user.mitxpro_application_user_id as source_user_id
        , dim_user.mitxonline_application_user_id as user_mitxonline_id
        -- The xPRO login email, not dim_user's email from the newest platform
        , lower(mitxpro_users.user_email) as user_email
        , xpro_runs.courserun_readable_id
        , mitxonline_runs.source_id as courserun_id
        , xpro_enrollment.enrollment_mode
        , xpro_enrollment.enrollment_is_active
        , xpro_enrollment.enrollment_created_on
        , xpro_grade.grade_value
        , xpro_grade.letter_grade
        , xpro_grade.is_passing
        , xpro_certificate.certificate_uuid
        , xpro_certificate.certificate_is_revoked
        , xpro_certificate.certificate_issued_on
        , case
            when
                mitxonline_enrollment.user_fk is null
                and xpro_enrollment.enrollment_is_active
                and dim_user.user_is_active_on_mitxpro
                then 'new'
            -- An inactive enrollment or account is not migrated
            when mitxonline_enrollment.user_fk is null then null
            when ({{ is_distinct_from('xpro_enrollment.enrollment_is_active', 'mitxonline_enrollment.enrollment_is_active') }})
                then 'status_changed'
            -- xPRO wins only if it changed the grade after MITx Online last did
            when
                xpro_grade.user_fk is not null
                and (
                    mitxonline_grade.user_fk is null
                    or (
                        xpro_grade.grade_changed_on > mitxonline_grade.grade_changed_on
                        and (
                            {{ is_distinct_from('xpro_grade.grade_value', 'mitxonline_grade.grade_value') }}
                            or {{ is_distinct_from('xpro_grade.is_passing', 'mitxonline_grade.is_passing') }}
                        )
                    )
                )
                then 'grade_changed'
            when
                xpro_certificate.user_fk is not null
                and ({{ is_distinct_from('xpro_certificate.certificate_is_revoked', 'mitxonline_certificate.certificate_is_revoked') }})
                then 'certificate_changed'
        end as migration_status
    from enrollments as xpro_enrollment
    inner join {{ ref('dim_user') }} as dim_user
        on xpro_enrollment.user_fk = dim_user.user_pk
    inner join {{ ref('int__mitxpro__users') }} as mitxpro_users
        on dim_user.mitxpro_application_user_id = mitxpro_users.user_id
    inner join {{ ref('dim_course_run') }} as xpro_runs
        on
            xpro_enrollment.courserun_fk = xpro_runs.courserun_pk
            and xpro_runs.platform = 'mitxpro'
            and xpro_runs.is_current = true
    left join {{ ref('dim_course_run') }} as mitxonline_runs
        on
            xpro_runs.courserun_readable_id = mitxonline_runs.courserun_readable_id
            and mitxonline_runs.platform = 'mitxonline'
            and mitxonline_runs.is_current = true
    left join enrollments as mitxonline_enrollment
        on
            xpro_enrollment.user_fk = mitxonline_enrollment.user_fk
            and mitxonline_runs.courserun_pk = mitxonline_enrollment.courserun_fk
            and mitxonline_enrollment.platform = 'mitxonline'
            and mitxonline_enrollment.row_num = 1
    left join grades as mitxonline_grade
        on
            xpro_enrollment.user_fk = mitxonline_grade.user_fk
            and mitxonline_runs.courserun_pk = mitxonline_grade.courserun_fk
            and mitxonline_grade.platform = 'mitxonline'
            and mitxonline_grade.row_num = 1
    left join certificates as mitxonline_certificate
        on
            xpro_enrollment.user_fk = mitxonline_certificate.user_fk
            and mitxonline_runs.courserun_pk = mitxonline_certificate.courserun_fk
            and mitxonline_certificate.platform = 'mitxonline'
            and mitxonline_certificate.row_num = 1
    left join grades as xpro_grade
        on
            xpro_enrollment.user_fk = xpro_grade.user_fk
            and xpro_enrollment.courserun_fk = xpro_grade.courserun_fk
            and xpro_grade.platform = 'mitxpro'
            and xpro_grade.row_num = 1
    left join certificates as xpro_certificate
        on
            xpro_enrollment.user_fk = xpro_certificate.user_fk
            and xpro_enrollment.courserun_fk = xpro_certificate.courserun_fk
            and xpro_certificate.platform = 'mitxpro'
            and xpro_certificate.row_num = 1
            -- A revoked certificate matters only if MITx Online has one to revoke
            and (xpro_certificate.certificate_is_revoked = false or mitxonline_certificate.user_fk is not null)
    where
        xpro_enrollment.platform = 'mitxpro'
        and xpro_enrollment.row_num = 1
        -- DEMO runs are staff previews
        and coalesce(xpro_runs.courserun_title, '') not like 'DEMO%'
)

, xpro_course_enrollments as (
    select
        'xpro' as source
        , migration_status
        , source_user_id
        , user_mitxonline_id
        , user_email
        , courserun_readable_id
        , courserun_id
        -- no-id-professional is xPRO's paid mode
        , case when enrollment_mode = 'audit' then 'audit' else 'verified' end as courserunenrollment_enrollment_mode
        , enrollment_is_active as courserunenrollment_is_active
        , enrollment_created_on as courserunenrollment_created_on
        , grade_value as courserungrade_grade
        , letter_grade as courserungrade_letter_grade
        , is_passing as courserungrade_is_passing
        , certificate_uuid as courseruncertificate_uuid
        , certificate_is_revoked as courseruncertificate_is_revoked
        , certificate_issued_on as courseruncertificate_created_on
    from xpro_compared
    where migration_status is not null
)

-- Add each new source as a CTE and union all it here
select
    source
    , migration_status
    , source_user_id
    , user_mitxonline_id
    , user_email
    , courserun_readable_id
    , courserun_id
    , courserunenrollment_enrollment_mode
    , courserunenrollment_is_active
    , courserunenrollment_created_on
    , courserungrade_grade
    , courserungrade_letter_grade
    , courserungrade_is_passing
    , courseruncertificate_uuid
    , courseruncertificate_is_revoked
    , courseruncertificate_created_on
from xpro_course_enrollments
