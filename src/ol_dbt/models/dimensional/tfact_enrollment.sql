{{ config(
    materialized='incremental',
    unique_key='enrollment_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Consolidate enrollments from all platforms
with mitxonline_enrollments as (
    select
        cast(courserunenrollment_id as varchar) as enrollment_id
        , user_id
        , courserun_id
        , cast(null as varchar) as courserun_readable_id  -- join key for edxorg only
        , null as program_id
        , courserunenrollment_created_on as enrollment_created_on
        , courserunenrollment_is_active as enrollment_is_active
        , courserunenrollment_enrollment_mode as enrollment_mode
        , courserunenrollment_enrollment_status as enrollment_status
        , 'mitxonline' as platform
        , 'mitxonline' as platform_code
    from {{ ref('int__mitxonline__courserunenrollments') }}
)

, mitxpro_enrollments as (
    select
        cast(courserunenrollment_id as varchar) as enrollment_id
        , user_id
        , courserun_id
        , cast(null as varchar) as courserun_readable_id  -- join key for edxorg only
        , null as program_id
        , courserunenrollment_created_on as enrollment_created_on
        , courserunenrollment_is_active as enrollment_is_active
        , courserunenrollment_enrollment_mode as enrollment_mode
        , null as enrollment_status
        , 'mitxpro' as platform
        , 'mitxpro' as platform_code
    from {{ ref('int__mitxpro__courserunenrollments') }}
)

, edxorg_enrollments as (
    select
        -- Stable surrogate key: edxorg has no enrollment_id; use natural key (user + course run)
        {{ dbt_utils.generate_surrogate_key(['cast(user_id as varchar)', 'courserun_readable_id']) }}
            as enrollment_id
        , user_id
        , null as courserun_id  -- edxorg has no integer source_id; join on readable_id instead
        , courserun_readable_id
        , null as program_id
        , courserunenrollment_created_on as enrollment_created_on
        , courserunenrollment_is_active as enrollment_is_active
        , courserunenrollment_enrollment_mode as enrollment_mode
        , null as enrollment_status
        , 'edxorg' as platform
        , 'edxorg' as platform_code
    from {{ ref('int__edxorg__mitx_courserun_enrollments') }}
)

-- NOTE: This CTE captures MITx Online program enrollments only. MicroMasters program
-- enrollments on edX.org (tracked in int__edxorg__mitx_program_enrollments) are not
-- included here because edxorg MicroMasters programs use a separate program ID namespace
-- that does not match the program IDs in dim_program. Linking those enrollments requires
-- a separate mapping effort and is tracked as future work.
, program_enrollments as (
    select
        cast(programenrollment_id as varchar) as enrollment_id
        , user_id
        , null as courserun_id
        , cast(null as varchar) as courserun_readable_id  -- join key for edxorg only
        , program_id
        , programenrollment_created_on as enrollment_created_on
        , programenrollment_is_active as enrollment_is_active
        , null as enrollment_mode
        , null as enrollment_status
        , 'mitxonline' as platform
        , 'mitxonline' as platform_code
    from {{ ref('int__mitxonline__programenrollments') }}
)

, residential_enrollments as (
    select
        cast(courserunenrollment_id as varchar) as enrollment_id
        , user_id
        , null as courserun_id
        , courserun_readable_id
        , null as program_id
        , courserunenrollment_created_on as enrollment_created_on
        , courserunenrollment_is_active as enrollment_is_active
        , courserunenrollment_enrollment_mode as enrollment_mode
        , null as enrollment_status
        , 'residential' as platform
        , 'residential' as platform_code
    from {{ ref('int__mitxresidential__courserun_enrollments') }}
)

, combined_enrollments as (
    select * from mitxonline_enrollments
    union all
    select * from mitxpro_enrollments
    union all
    select * from edxorg_enrollments
    union all
    select * from residential_enrollments
    union all
    select * from program_enrollments
)

-- Join to dimensions for FKs
-- dim_user not in Phase 1-2

, user_lookup as (
    select
        user_pk
        , mitxonline_application_user_id
        , mitxpro_application_user_id
        , edxorg_openedx_user_id
        , micromasters_user_id
        , residential_openedx_user_id
    from {{ ref('dim_user') }}
    where user_pk is not null
)

, dim_course_run as (
    select courserun_pk, source_id, courserun_readable_id, platform_fk, platform
    from {{ ref('dim_course_run') }}
    where is_current = true
)

, dim_program as (
    select program_pk, source_id, platform_code
    from {{ ref('dim_program') }}
)

-- MicroMasters program lookup: maps courserun_readable_id to micromasters program_pk.
-- Used to enrich edxorg/mitxonline enrollment rows that belong to MM programs.
, micromasters_program_lookup as (
    select distinct
        courserun_readable_id
        , dim_program.program_pk as micromasters_program_pk
    from {{ ref('int__micromasters__course_enrollments') }} as mm_enroll
    inner join dim_program
        on cast(mm_enroll.micromasters_program_id as varchar) = cast(dim_program.source_id as varchar)
        and dim_program.platform_code = 'micromasters'
)

-- dim_platform not in Phase 1-2

, dim_platform_lookup as (
    select platform_pk, platform_readable_id
    from {{ ref('dim_platform') }}
)

, enrollments_with_fks as (
    select
        combined_enrollments.*
        , coalesce(
            case when combined_enrollments.platform = 'mitxonline'
                then ul_mitxonline.user_pk
            end,
            case when combined_enrollments.platform = 'mitxpro'
                then ul_mitxpro.user_pk
            end,
            case when combined_enrollments.platform = 'edxorg'
                then ul_edxorg.user_pk
            end,
            case when combined_enrollments.platform = 'residential'
                then ul_residential.user_pk
            end
        ) as user_fk
        , dim_course_run.courserun_pk as courserun_fk
        , coalesce(dim_program.program_pk, micromasters_program_lookup.micromasters_program_pk) as program_fk
        , dim_platform_lookup.platform_pk as platform_fk
        , {{ iso8601_to_date_key('enrollment_created_on') }} as enrollment_date_key
    from combined_enrollments
    left join user_lookup as ul_mitxonline
        on combined_enrollments.platform = 'mitxonline'
        and combined_enrollments.user_id = ul_mitxonline.mitxonline_application_user_id
    left join user_lookup as ul_mitxpro
        on combined_enrollments.platform = 'mitxpro'
        and combined_enrollments.user_id = ul_mitxpro.mitxpro_application_user_id
    left join user_lookup as ul_edxorg
        on combined_enrollments.platform = 'edxorg'
        and combined_enrollments.user_id = ul_edxorg.edxorg_openedx_user_id
    left join user_lookup as ul_residential
        on combined_enrollments.platform = 'residential'
        and combined_enrollments.user_id = ul_residential.residential_openedx_user_id
    left join dim_course_run
        on (
            -- mitxonline/mitxpro: join on integer source_id
            (combined_enrollments.platform in ('mitxonline', 'mitxpro') and combined_enrollments.courserun_id = dim_course_run.source_id and combined_enrollments.platform = dim_course_run.platform)
            -- edxorg, residential: join on readable_id
            or (combined_enrollments.platform in ('edxorg', 'residential') and combined_enrollments.courserun_readable_id = dim_course_run.courserun_readable_id)
        )
    left join dim_program
        on combined_enrollments.program_id = dim_program.source_id
        and combined_enrollments.platform_code = dim_program.platform_code
    left join micromasters_program_lookup
        on combined_enrollments.courserun_readable_id = micromasters_program_lookup.courserun_readable_id
    left join dim_platform_lookup
        on combined_enrollments.platform = dim_platform_lookup.platform_readable_id
)

, final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cast(enrollment_id as varchar)',
            'platform',
            "case when program_id is not null then 'program' else 'course' end"
        ]) }} as enrollment_key
        , enrollment_id
        , enrollment_date_key
        , user_fk
        , courserun_fk
        , program_fk
        , platform_fk
        , platform
        , enrollment_is_active
        , enrollment_mode
        , enrollment_status
        , enrollment_created_on
    from enrollments_with_fks

    {% if is_incremental() %}
    where (
        enrollment_created_on > (
            select max(enrollment_created_on) from {{ this }}
            where platform = enrollments_with_fks.platform
        )
        or enrollment_created_on is null
    )
    {% endif %}
)

select * from final
