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
        , courserun_readable_id
        , null as program_id
        , 'course' as enrollment_scope
        , courserunenrollment_created_on as enrollment_created_on
        , courserunenrollment_updated_on as enrollment_updated_on
        , courserunenrollment_is_active as enrollment_is_active
        , courserunenrollment_enrollment_mode as enrollment_mode
        , courserunenrollment_enrollment_status as enrollment_status
        , 'mitxonline' as platform
        , 'mitxonline' as platform_code
        , courserunenrollment_is_edx_enrolled as enrollment_is_edx_enrolled
        -- mitxonline has no order FK on the enrollment record; order attribution for
        -- this platform relies on product-matching in downstream consumers.
        , cast(null as bigint) as order_id
    from {{ ref('int__mitxonline__courserunenrollments') }}
)

-- A program-purchase order creates one ProgramEnrollment plus one CourseRunEnrollment per
-- course in the program, all sharing the same ecommerce_order_id. Use that shared order to
-- recover the program_id for course-run enrollments that came from a program order.
, mitxpro_program_enrollments_by_order as (
    select
        ecommerce_order_id
        , program_id
        , row_number() over (
            partition by ecommerce_order_id
            order by programenrollment_created_on desc, programenrollment_id desc
        ) as row_num
    from {{ ref('int__mitxpro__programenrollments') }}
    where ecommerce_order_id is not null
)

, mitxpro_enrollments as (
    select
        cast(enrollments.courserunenrollment_id as varchar) as enrollment_id
        , enrollments.user_id
        , enrollments.courserun_id
        , enrollments.courserun_readable_id
        , mitxpro_program_enrollments_by_order.program_id
        , 'course' as enrollment_scope
        , enrollments.courserunenrollment_created_on as enrollment_created_on
        , enrollments.courserunenrollment_updated_on as enrollment_updated_on
        , enrollments.courserunenrollment_is_active as enrollment_is_active
        , enrollments.courserunenrollment_enrollment_mode as enrollment_mode
        , enrollments.courserunenrollment_enrollment_status as enrollment_status
        , 'mitxpro' as platform
        , 'mitxpro' as platform_code
        , enrollments.courserunenrollment_is_edx_enrolled as enrollment_is_edx_enrolled
        -- xPRO is the only platform that stores an order FK directly on the enrollment
        -- record. This is authoritative regardless of subsequent deferral to a different
        -- course run or bundling into a program purchase — downstream consumers should
        -- prefer this over reconstructing order attribution via product-matching.
        , cast(enrollments.ecommerce_order_id as bigint) as order_id
    from {{ ref('int__mitxpro__courserunenrollments') }} as enrollments
    left join mitxpro_program_enrollments_by_order
        on
            enrollments.ecommerce_order_id = mitxpro_program_enrollments_by_order.ecommerce_order_id
            and mitxpro_program_enrollments_by_order.row_num = 1
)

, edxorg_enrollments as (
    select
        -- Stable surrogate key: edxorg has no enrollment_id; use natural key (user + course run)
        {{ dbt_utils.generate_surrogate_key(['cast(user_id as varchar)', 'courserun_readable_id']) }} as enrollment_id
        , user_id
        , null as courserun_id  -- edxorg has no integer source_id
        , courserun_readable_id
        , null as program_id
        , 'course' as enrollment_scope
        , courserunenrollment_created_on as enrollment_created_on
        , cast(null as varchar) as enrollment_updated_on
        , courserunenrollment_is_active as enrollment_is_active
        , courserunenrollment_enrollment_mode as enrollment_mode
        , null as enrollment_status
        , 'edxorg' as platform
        , 'edxorg' as platform_code
        , true as enrollment_is_edx_enrolled
        , cast(null as bigint) as order_id
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
        , cast(null as varchar) as courserun_readable_id
        , program_id
        , 'program' as enrollment_scope
        , programenrollment_created_on as enrollment_created_on
        , programenrollment_updated_on as enrollment_updated_on
        , programenrollment_is_active as enrollment_is_active
        , programenrollment_enrollment_mode as enrollment_mode
        , programenrollment_enrollment_status as enrollment_status
        , 'mitxonline' as platform
        , 'mitxonline' as platform_code
        , cast(null as boolean) as enrollment_is_edx_enrolled
        , cast(null as bigint) as order_id
    from {{ ref('int__mitxonline__programenrollments') }}
)

, residential_enrollments as (
    select
        cast(courserunenrollment_id as varchar) as enrollment_id
        , user_id
        , null as courserun_id
        , courserun_readable_id
        , null as program_id
        , 'course' as enrollment_scope
        , courserunenrollment_created_on as enrollment_created_on
        , cast(null as varchar) as enrollment_updated_on
        , courserunenrollment_is_active as enrollment_is_active
        , courserunenrollment_enrollment_mode as enrollment_mode
        , null as enrollment_status
        , 'residential' as platform
        , 'residential' as platform_code
        , true as enrollment_is_edx_enrolled
        , cast(null as bigint) as order_id
    from {{ ref('int__mitxresidential__courserun_enrollments') }}
)

, bootcamps_enrollments as (
    select
        cast(courserunenrollment_id as varchar) as enrollment_id
        , user_id
        , courserun_id
        , courserun_readable_id
        , null as program_id
        , 'course' as enrollment_scope
        , courserunenrollment_created_on as enrollment_created_on
        , courserunenrollment_updated_on as enrollment_updated_on
        , courserunenrollment_is_active as enrollment_is_active
        , cast(null as varchar) as enrollment_mode
        , courserunenrollment_enrollment_status as enrollment_status
        , 'bootcamps' as platform
        , 'bootcamps' as platform_code
        , cast(null as boolean) as enrollment_is_edx_enrolled
        , cast(null as bigint) as order_id
    from {{ ref('int__bootcamps__courserunenrollments') }}
)

-- Emeritus/Global Alumni don't own course run records; their enrollments map onto
-- existing MITxPro course runs via courserun_external_readable_id, mirroring the join
-- logic in int__combined__courserun_enrollments. This lookup resolves that external id
-- to the MITxPro-platform courserun_readable_id used elsewhere in dim_course_run.
, mitxpro_external_readable_id_lookup as (
    select courserun_external_readable_id, courserun_readable_id
    from {{ ref('int__mitxpro__course_runs') }}
    where courserun_external_readable_id is not null
)

, emeritus_enrollments as (
    select
        -- Stable surrogate key: source has no native enrollment_id
        {{ dbt_utils.generate_surrogate_key(['cast(emeritus_enrollments.user_id as varchar)', 'emeritus_enrollments.courserun_external_readable_id']) }}
            as enrollment_id
        , emeritus_enrollments.user_id
        , null as courserun_id
        , coalesce(
            mitxpro_external_readable_id_lookup.courserun_readable_id
            , emeritus_enrollments.courserun_external_readable_id
        ) as courserun_readable_id
        , null as program_id
        , emeritus_enrollments.enrollment_created_on
        , emeritus_enrollments.enrollment_updated_on
        , emeritus_enrollments.is_enrolled as enrollment_is_active
        , cast(null as varchar) as enrollment_mode
        , emeritus_enrollments.enrollment_status
        , 'emeritus' as platform
        , 'emeritus' as platform_code
        , cast(null as boolean) as enrollment_is_edx_enrolled
    from {{ ref('stg__emeritus__api__bigquery__user_enrollments') }} as emeritus_enrollments
    left join mitxpro_external_readable_id_lookup
        on emeritus_enrollments.courserun_external_readable_id
            = mitxpro_external_readable_id_lookup.courserun_external_readable_id
)

, global_alumni_enrollments as (
    select
        -- Stable surrogate key: source has no native enrollment_id
        {{ dbt_utils.generate_surrogate_key(['cast(global_alumni_enrollments.user_id as varchar)', 'global_alumni_enrollments.courserun_external_readable_id']) }}
            as enrollment_id
        , global_alumni_enrollments.user_id
        , null as courserun_id
        , coalesce(
            mitxpro_external_readable_id_lookup.courserun_readable_id
            , global_alumni_enrollments.courserun_external_readable_id
        ) as courserun_readable_id
        , null as program_id
        -- source has no enrollment_created_on/enrollment_updated_on. Unlike edxorg/residential
        -- (which have enrollment_created_on and use the 7-day lookback path), the unconditional
        -- `or ewf.enrollment_created_on is null` branch in incremental_watermarks means every
        -- Global Alumni row is reprocessed on every incremental run (volume is low, so this is
        -- an acceptable tradeoff for now; revisit if an ingestion timestamp becomes available
        -- upstream).
        , cast(null as varchar) as enrollment_created_on
        , cast(null as varchar) as enrollment_updated_on
        , global_alumni_enrollments.is_enrolled as enrollment_is_active
        , cast(null as varchar) as enrollment_mode
        , global_alumni_enrollments.enrollment_status
        , 'global_alumni' as platform
        , 'global_alumni' as platform_code
        , cast(null as boolean) as enrollment_is_edx_enrolled
    from {{ ref('stg__global_alumni__api__bigquery__user_enrollments') }} as global_alumni_enrollments
    left join mitxpro_external_readable_id_lookup
        on global_alumni_enrollments.courserun_external_readable_id
            = mitxpro_external_readable_id_lookup.courserun_external_readable_id
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
    union all
    select * from bootcamps_enrollments
    union all
    select * from emeritus_enrollments
    union all
    select * from global_alumni_enrollments
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
        , bootcamps_application_user_id
        , emeritus_user_id
        , global_alumni_user_id
    from {{ ref('dim_user') }}
    where user_pk is not null
)

, dim_course_run as (
    select courserun_pk, courserun_readable_id, platform
    from {{ ref('dim_course_run') }}
    where is_current = true
)

-- Emeritus/Global Alumni enrollments resolve their course run FK against MITxPro-platform
-- dim_course_run rows (see mitxpro_external_readable_id_lookup above).
, dim_course_run_mitxpro as (
    select courserun_pk, courserun_readable_id
    from {{ ref('dim_course_run') }}
    where is_current = true and platform = 'mitxpro'
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
            end,
            case when combined_enrollments.platform = 'bootcamps'
                then ul_bootcamps.user_pk
            end,
            case when combined_enrollments.platform = 'emeritus'
                then ul_emeritus.user_pk
            end,
            case when combined_enrollments.platform = 'global_alumni'
                then ul_global_alumni.user_pk
            end
        ) as user_fk
        , coalesce(dim_course_run.courserun_pk, dim_course_run_mitxpro.courserun_pk) as courserun_fk
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
    left join user_lookup as ul_bootcamps
        on combined_enrollments.platform = 'bootcamps'
        and combined_enrollments.user_id = ul_bootcamps.bootcamps_application_user_id
    left join user_lookup as ul_emeritus
        on combined_enrollments.platform = 'emeritus'
        and combined_enrollments.user_id = ul_emeritus.emeritus_user_id
    left join user_lookup as ul_global_alumni
        on combined_enrollments.platform = 'global_alumni'
        and combined_enrollments.user_id = ul_global_alumni.global_alumni_user_id
    left join dim_course_run
        on combined_enrollments.courserun_readable_id = dim_course_run.courserun_readable_id
        and combined_enrollments.platform = dim_course_run.platform
    left join dim_course_run_mitxpro
        on combined_enrollments.platform in ('emeritus', 'global_alumni')
        and combined_enrollments.courserun_readable_id = dim_course_run_mitxpro.courserun_readable_id
    left join dim_program
        on cast(combined_enrollments.program_id as varchar) = dim_program.source_id
        and combined_enrollments.platform_code = dim_program.platform_code
    left join micromasters_program_lookup
        on combined_enrollments.courserun_readable_id = micromasters_program_lookup.courserun_readable_id
    left join dim_platform_lookup
        on combined_enrollments.platform = dim_platform_lookup.platform_readable_id
)

{% if is_incremental() %}
-- Pre-compute per-(platform, enrollment_type) watermarks to avoid the correlated
-- subquery anti-pattern. Without this, Trino executes the WHERE as:
--   AssignUniqueId + LeftJoin(all target rows on platform+type) + StreamingAggregate
-- which fans out to billions of intermediate rows.
-- Use max(enrollment_updated_on) where available (program enrollments), falling back
-- to max(enrollment_created_on), so status/mode changes are captured incrementally.
, incremental_watermarks as (
    select
        platform as watermark_platform
        , enrollment_type as watermark_enrollment_type
        , max(coalesce(enrollment_updated_on, enrollment_created_on)) as max_activity_on
    from {{ this }}
    group by platform, enrollment_type
)
{% endif %}

, final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cast(enrollment_id as varchar)',
            'platform',
            'enrollment_scope'
        ]) }} as enrollment_key
        , enrollment_id
        , enrollment_scope as enrollment_type
        , enrollment_date_key
        , user_fk
        , courserun_fk
        , program_fk
        , platform_fk
        , ewf.platform
        , enrollment_is_active
        , enrollment_mode
        , enrollment_status
        , enrollment_created_on
        , enrollment_updated_on
        , enrollment_is_edx_enrolled
        , order_id
    from enrollments_with_fks as ewf

    {% if is_incremental() %}
    -- left join preserves enrollments from platforms/types not yet in the target table
    left join incremental_watermarks w
        on w.watermark_platform = ewf.platform
        and w.watermark_enrollment_type = ewf.enrollment_scope
    where (
        w.max_activity_on is null  -- platform/type not yet in target, include all
        -- Use >= for updated_on watermark: updated_on can equal max on state changes within same second
        or coalesce(ewf.enrollment_updated_on, ewf.enrollment_created_on) >= w.max_activity_on
        -- For platforms without updated_on (edxorg, residential), apply a 7-day lookback to catch
        -- status changes (deactivations, mode upgrades) that don't bump enrollment_created_on
        or (
            ewf.enrollment_updated_on is null
            and ewf.enrollment_created_on is not null
            and ewf.enrollment_created_on >= {{ cast_timestamp_to_iso8601("current_timestamp - interval '7' day") }}
        )
        or ewf.enrollment_created_on is null
    )
    {% endif %}
)

-- Defensive dedup: the UNION ALL across 5 platform CTEs has no upstream uniqueness guarantee.
-- If any intermediate develops grain drift, this guard prevents duplicate enrollment_key values
-- from silently entering the fact table and corrupting incremental MERGE operations.
-- Note: QUALIFY is not supported by Trino; using ROW_NUMBER subquery instead.
, final_deduped as (
    select
        enrollment_key
        , enrollment_id
        , enrollment_type
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
        , enrollment_updated_on
        , enrollment_is_edx_enrolled
        , order_id
        , row_number() over (
            partition by enrollment_key
            order by coalesce(enrollment_updated_on, enrollment_created_on) desc nulls last
        ) as _row_num
    from final
)

select
    enrollment_key
    , enrollment_id
    , enrollment_type
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
    , enrollment_updated_on
    , enrollment_is_edx_enrolled
    , order_id
from final_deduped
where _row_num = 1
