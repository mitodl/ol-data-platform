{{ config(
    materialized='incremental',
    unique_key='certificate_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Consolidate certificates from all platforms
with mitxonline_certificates as (
    select
        cast(courseruncertificate_id as varchar) as certificate_id
        , user_id
        , courserun_id
        , courserun_readable_id
        , courseruncertificate_uuid as certificate_uuid
        , courseruncertificate_is_revoked as certificate_is_revoked
        , courseruncertificate_created_on as certificate_created_on
        , courseruncertificate_updated_on as certificate_updated_on
        , courseruncertificate_issued_on as certificate_issued_on
        , 'verified' as certificate_type_code
        , 'mitxonline' as platform
        , cast(null as varchar) as user_email  -- micromasters join key only
        , cast(null as varchar) as program_id
    from {{ ref('int__mitxonline__courserun_certificates') }}
)

, mitxpro_certificates as (
    select
        cast(courseruncertificate_id as varchar) as certificate_id
        , user_id
        , courserun_id
        , courserun_readable_id
        , courseruncertificate_uuid as certificate_uuid
        , courseruncertificate_is_revoked as certificate_is_revoked
        , courseruncertificate_created_on as certificate_created_on
        , courseruncertificate_updated_on as certificate_updated_on
        , courseruncertificate_created_on as certificate_issued_on
        , 'professional' as certificate_type_code
        , 'mitxpro' as platform
        , cast(null as varchar) as user_email  -- micromasters join key only
        , cast(null as varchar) as program_id
    from {{ ref('int__mitxpro__courserun_certificates') }}
)

, edxorg_certificates as (
    select
        {{ dbt_utils.generate_surrogate_key(['cast(user_id as varchar)', 'courserun_readable_id']) }} as certificate_id
        , user_id
        , cast(null as integer) as courserun_id  -- edxorg has no integer source_id
        , courserun_readable_id
        , cast(null as varchar) as certificate_uuid
        , false as certificate_is_revoked  -- edxorg doesn't track revocations
        , courseruncertificate_created_on as certificate_created_on
        , courseruncertificate_updated_on as certificate_updated_on
        , courseruncertificate_created_on as certificate_issued_on
        , courseruncertificate_mode as certificate_type_code
        , 'edxorg' as platform
        , cast(null as varchar) as user_email  -- micromasters join key only
        , cast(null as varchar) as program_id
    from {{ ref('int__edxorg__mitx_courserun_certificates') }}
)

, micromasters_certificates as (
    select
        {{ dbt_utils.generate_surrogate_key(['user_email', 'courserun_readable_id']) }} as certificate_id
        , cast(null as integer) as user_id  -- no integer user ID; resolved via user_email
        , cast(null as integer) as courserun_id
        , courserun_readable_id
        , courseruncertificate_uuid as certificate_uuid
        , false as certificate_is_revoked
        , courseruncertificate_created_on as certificate_created_on
        , courseruncertificate_created_on as certificate_updated_on   --- micromasters only has created_on timestamp
        , courseruncertificate_created_on as certificate_issued_on
        , 'verified' as certificate_type_code
        , 'micromasters' as platform
        , user_email
        , cast(null as varchar) as program_id
    from {{ ref('int__micromasters__course_certificates') }}
)

, mitxonline_program_certificates as (
    select
        cast(programcertificate_id as varchar) as certificate_id
        , user_id
        , cast(null as integer) as courserun_id
        , cast(null as varchar) as courserun_readable_id
        , programcertificate_uuid as certificate_uuid
        , programcertificate_is_revoked as certificate_is_revoked
        , programcertificate_created_on as certificate_created_on
        , programcertificate_updated_on as certificate_updated_on
        , programcertificate_issued_on as certificate_issued_on
        , 'verified' as certificate_type_code
        , 'mitxonline' as platform
        , user_email
        , cast(program_id as varchar) as program_id
    from {{ ref('int__mitxonline__program_certificates') }}
)

, mitxpro_program_certificates as (
    select
        cast(programcertificate_id as varchar) as certificate_id
        , user_id
        , cast(null as integer) as courserun_id
        , cast(null as varchar) as courserun_readable_id
        , programcertificate_uuid as certificate_uuid
        , programcertificate_is_revoked as certificate_is_revoked
        , programcertificate_created_on as certificate_created_on
        , programcertificate_updated_on as certificate_updated_on
        , programcertificate_created_on as certificate_issued_on
        , 'professional' as certificate_type_code
        , 'mitxpro' as platform
        , user_email
        , cast(program_id as varchar) as program_id
    from {{ ref('int__mitxpro__program_certificates') }}
)

, edxorg_program_certificates as (
    select
        program_certificate_hashed_id as certificate_id
        , user_id
        , cast(null as integer) as courserun_id
        , cast(null as varchar) as courserun_readable_id
        , program_certificate_hashed_id as certificate_uuid
        , false as certificate_is_revoked
        , program_certificate_awarded_on as certificate_created_on
        , program_certificate_awarded_on as certificate_updated_on  -- edxorg only has awarded_on timestamp
        , program_certificate_awarded_on as certificate_issued_on
        , 'verified' as certificate_type_code
        , 'edxorg' as platform
        , cast(null as varchar) as user_email  -- micromasters join key only
        , program_uuid as program_id -- matches dim_program.source_id as edxorg doesn't have integer program IDs
    from {{ ref('int__edxorg__mitx_program_certificates') }}
)

, bootcamps_certificates as (
    select
        cast(courseruncertificate_id as varchar) as certificate_id
        , user_id
        , courserun_id
        , courserun_readable_id
        , courseruncertificate_uuid as certificate_uuid
        , courseruncertificate_is_revoked as certificate_is_revoked
        , courseruncertificate_created_on as certificate_created_on
        , courseruncertificate_updated_on as certificate_updated_on
        , courseruncertificate_created_on as certificate_issued_on  -- no issued_on; fall back to created_on
        , 'verified' as certificate_type_code
        , 'bootcamps' as platform
        , user_email
        , cast(null as varchar) as program_id
    from {{ ref('int__bootcamps__courserun_certificates') }}
)

, combined_certificates as (
    select * from mitxonline_certificates
    union all
    select * from mitxpro_certificates
    union all
    select * from edxorg_certificates
    union all
    select * from micromasters_certificates
    union all
    select * from mitxonline_program_certificates
    union all
    select * from mitxpro_program_certificates
    union all
    select * from edxorg_program_certificates
    union all
    select * from bootcamps_certificates
)

, user_lookup as (
    select
        user_pk
        , email
        , mitxonline_application_user_id
        , mitxpro_application_user_id
        , edxorg_openedx_user_id
        , bootcamps_application_user_id
    from {{ ref('dim_user') }}
    where user_pk is not null
)

, dim_course_run as (
    select courserun_pk, courserun_readable_id, platform
    from {{ ref('dim_course_run') }}
    where is_current = true
)

-- dim_platform not in Phase 1-2
, dim_platform_lookup as (
    select platform_pk, platform_readable_id
    from {{ ref('dim_platform') }}
)

, dim_certificate_type as (
    select certificate_type_pk, certificate_type_code
    from {{ ref('dim_certificate_type') }}
)

, dim_program as (
    select program_pk, source_id, platform_code
    from {{ ref('dim_program') }}
)

, certificates_with_fks as (
    select
        combined_certificates.*
        , coalesce(
            case when combined_certificates.platform = 'mitxonline'
                then ul_mitxonline.user_pk
            end,
            case when combined_certificates.platform = 'mitxpro'
                then ul_mitxpro.user_pk
            end,
            case when combined_certificates.platform = 'edxorg'
                then ul_edxorg.user_pk
            end,
            case when combined_certificates.platform = 'micromasters'
                then ul_micromasters.user_pk
            end,
            case when combined_certificates.platform = 'bootcamps'
                then ul_bootcamps.user_pk
            end
        ) as user_fk
        , dim_course_run.courserun_pk as courserun_fk
        , dim_platform_lookup.platform_pk as platform_fk
        , dim_certificate_type.certificate_type_pk as certificate_type_fk
        , dim_program.program_pk as program_fk
        , case when combined_certificates.program_id is not null then 'program' else 'course' end as certificate_scope
        , {{ iso8601_to_date_key('certificate_issued_on') }} as certificate_issued_date_key
    from combined_certificates
    left join user_lookup as ul_mitxonline
        on combined_certificates.platform = 'mitxonline'
        and combined_certificates.user_id = ul_mitxonline.mitxonline_application_user_id
    left join user_lookup as ul_mitxpro
        on combined_certificates.platform = 'mitxpro'
        and combined_certificates.user_id = ul_mitxpro.mitxpro_application_user_id
    left join user_lookup as ul_edxorg
        on combined_certificates.platform = 'edxorg'
        and combined_certificates.user_id = ul_edxorg.edxorg_openedx_user_id
    left join user_lookup as ul_micromasters
        on combined_certificates.platform = 'micromasters'
        -- dim_user.email is always lower()-ed; micromasters emails are not,
        -- so a mixed-case email would otherwise silently fail to resolve user_fk
        and lower(combined_certificates.user_email) = ul_micromasters.email
    left join user_lookup as ul_bootcamps
        on combined_certificates.platform = 'bootcamps'
        and combined_certificates.user_id = ul_bootcamps.bootcamps_application_user_id
    left join dim_course_run
        on combined_certificates.courserun_readable_id = dim_course_run.courserun_readable_id
        and case
            when combined_certificates.platform = 'micromasters' then 'edxorg'
            else combined_certificates.platform
        end = dim_course_run.platform
    left join dim_platform_lookup
        on combined_certificates.platform = dim_platform_lookup.platform_readable_id
    left join dim_certificate_type
        on dim_certificate_type.certificate_type_code = combined_certificates.certificate_type_code
    left join dim_program
        on combined_certificates.program_id = dim_program.source_id
        and combined_certificates.platform = dim_program.platform_code
)

-- MicroMasters course certificates are earned on and issued by edX.org, and both
-- the edxorg and micromasters sources resolve courserun_fk to the same edxorg
-- course run (see the dim_course_run join above). Their surrogate certificate_ids
-- differ (user_id-based vs email-based), so the same physical certificate enters
-- as two rows and the certificate_key-based defensive dedup further below can't
-- catch it. Collapse to one row per (user_fk, courserun_fk, certificate_scope)
-- when both FKs resolved, preferring the canonical edxorg record.
, cross_source_deduped as (
    select
        *
        , row_number() over (
            partition by
                case
                    when user_fk is not null and courserun_fk is not null
                        then concat(cast(user_fk as varchar), '|', cast(courserun_fk as varchar), '|', certificate_scope)
                    else concat(certificate_id, '|', platform, '|', certificate_scope)
                end
            order by
                case platform
                    when 'edxorg' then 0
                    when 'micromasters' then 1
                    else 0
                end
        ) as _cross_source_row_num
    from certificates_with_fks
)

{% if is_incremental() %}
-- Pre-compute per-platform watermarks in a single scan of {{ this }}.
-- The correlated subquery pattern (WHERE platform = outer.platform) causes
-- Trino to execute AssignUniqueId + LeftJoin(all target rows on platform) +
-- StreamingAggregate, producing a 25B-row intermediate at 7TB.
-- A pre-computed CTE + regular equijoin eliminates that fan-out entirely.
, incremental_watermarks as (
    select
        platform as watermark_platform
         , certificate_scope as watermark_certificate_type
         , max(coalesce(certificate_updated_on, certificate_created_on)) as max_activity_on
    from {{ this }}
    group by platform, certificate_scope)
{% endif %}

, final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cast(certificate_id as varchar)',
            'platform',
            'certificate_scope'
        ]) }} as certificate_key
        , certificate_id
        , certificate_issued_date_key
        , user_fk
        , courserun_fk
        , program_fk
        , platform_fk
        , certificate_type_fk
        , cwf.platform
        , cwf.certificate_scope
        , certificate_uuid
        , certificate_is_revoked
        , certificate_created_on
        , certificate_updated_on
        , certificate_issued_on
    from cross_source_deduped as cwf
    where cwf._cross_source_row_num = 1

    {% if is_incremental() %}
    -- left join preserves certificates from platforms not yet in the target table
    left join incremental_watermarks w
        on w.watermark_platform = cwf.platform
        and w.watermark_certificate_type = cwf.certificate_scope
    where (
        w.max_activity_on is null  -- platform/type not yet in target, include all
        or coalesce(cwf.certificate_updated_on, cwf.certificate_created_on) >= w.max_activity_on
        or cwf.certificate_created_on is null
    )
    {% endif %}
)

-- Defensive dedup: the UNION ALL across 4 platform CTEs has no upstream uniqueness guarantee.
-- If any intermediate develops grain drift, this guard prevents duplicate certificate_key values
-- from silently entering the fact table and corrupting incremental MERGE operations.
-- Note: QUALIFY is not supported by Trino; using ROW_NUMBER subquery instead.
, final_deduped as (
    select
        certificate_key
        , certificate_id
        , certificate_issued_date_key
        , user_fk
        , courserun_fk
        , program_fk
        , platform_fk
        , certificate_type_fk
        , platform
        , certificate_scope
        , certificate_uuid
        , certificate_is_revoked
        , certificate_created_on
        , certificate_updated_on
        , certificate_issued_on
        , row_number() over (
            partition by certificate_key
            order by coalesce(certificate_updated_on, certificate_created_on) desc nulls last
        ) as _row_num
    from final
)

select
    certificate_key
    , certificate_id
    , certificate_issued_date_key
    , user_fk
    , courserun_fk
    , program_fk
    , platform_fk
    , certificate_type_fk
    , platform
    , certificate_scope
    , certificate_uuid
    , certificate_is_revoked
    , certificate_created_on
    , certificate_updated_on
    , certificate_issued_on
from final_deduped
where _row_num = 1
