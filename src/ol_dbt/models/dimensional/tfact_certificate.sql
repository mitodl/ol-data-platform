{{ config(
    materialized='incremental',
    unique_key='certificate_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

{#- The first incremental run after is_current is added reads a target without the column.
    In a unit test `this` is the fixture's name, a string, which always has it. -#}
{%- set target_has_is_current = false -%}
{%- if is_incremental() and this is string -%}
    {%- set target_has_is_current = true -%}
{%- elif is_incremental() -%}
    {%- set target_columns = adapter.get_columns_in_relation(this) | map(attribute='name') | map('lower') | list -%}
    {%- set target_has_is_current = 'is_current' in target_columns -%}
{%- endif %}

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
        -- so a mixed-case email would otherwise silently fail to resolve user_fk.
        -- lower() both sides defensively since it's unclear from this query alone
        -- that dim_user.email is guaranteed lowercase.
        and lower(combined_certificates.user_email) = lower(ul_micromasters.email)
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

-- One certificate can enter twice under the same certificate_key (the joins above have
-- no uniqueness guarantee). Reduce to one row per key before ranking: ranked together, the
-- second copy would be marked not current and the certificate would lose is_current to
-- itself.
, certificates_keyed as (
    select
        *
        , row_number() over (
            partition by certificate_id, platform, certificate_scope
            order by
                coalesce(certificate_updated_on, certificate_created_on) desc nulls last
                , user_fk
                , courserun_fk
                , program_fk
        ) as _key_row_num
    from certificates_with_fks
)

-- MicroMasters course certificates are earned on and issued by edX.org, and both
-- the edxorg and micromasters sources resolve courserun_fk to the same edxorg
-- course run (see the dim_course_run join above). Their surrogate certificate_ids
-- differ (user_id-based vs email-based), so the same physical certificate enters
-- as two rows and the certificate_key-based defensive dedup further below can't
-- catch it. The micromasters copy is dropped when an edxorg record exists for the same
-- (user_fk, courserun_fk, certificate_scope) and both FKs resolved.
-- One platform can also hold two certificates for a (user, course run): MITx Online has
-- a pair issued two days apart, and a revoked certificate can be followed by a new one.
-- Those are separate certificates and are all kept. is_current marks the one that stands:
-- unrevoked first, then the latest issued. A consumer joining on (user_fk, courserun_fk)
-- filters on is_current to get one row.
, certificates_grouped as (
    select
        *
        , case
            when user_fk is not null and courserun_fk is not null
                then concat(cast(user_fk as varchar), '|', cast(courserun_fk as varchar), '|', certificate_scope)
            else concat(certificate_id, '|', platform, '|', certificate_scope)
        end as _certificate_group
        , case when platform = 'micromasters' then 1 else 0 end as _platform_rank
    from certificates_keyed
    where _key_row_num = 1
)

, certificates_ranked as (
    select
        *
        , min(_platform_rank) over (partition by _certificate_group) as _best_platform_rank
        , row_number() over (
            partition by _certificate_group
            order by
                _platform_rank
                , case when certificate_is_revoked then 1 else 0 end
                , certificate_issued_on desc nulls last
                -- certificate_id is a string; compare the integer ids as numbers.
                , try_cast(certificate_id as bigint) desc nulls last
                , certificate_id desc
        ) as _cross_source_row_num
    from certificates_grouped
)

, cross_source_deduped as (
    select *
    from certificates_ranked
    where _platform_rank = _best_platform_rank
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

-- Snapshot of the target's current user_fk and is_current per certificate_key, used to
-- re-select rows where either has gone stale: user_fk after a dim_user re-key, is_current
-- when another certificate takes it. The source row itself did not change in either case,
-- so the activity-timestamp watermark alone would never catch it.
, stale_user_fk_lookup as (
    select
        certificate_key
        , user_fk as stored_user_fk
        , {% if target_has_is_current %}is_current{% else %}cast(null as boolean){% endif %} as stored_is_current
    from {{ this }}
)

-- If a platform's source model is empty for a run, nothing below treats its certificates
-- as gone: only a (platform, scope) the source still produces can lose rows. Read before
-- the micromasters copies are dropped, or micromasters would look empty once every one of
-- its certificates has an edxorg record, and its leftover copies would stay current.
, source_platforms as (
    select distinct
        platform
        , certificate_scope
    from certificates_with_fks
)
{% endif %}

, final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cast(certificate_id as varchar)',
            'platform',
            'certificate_scope'
        ]) }} as certificate_key
        , cwf.certificate_id
        , cwf.certificate_issued_date_key
        , cwf.user_fk
        , cwf.courserun_fk
        , cwf.program_fk
        , cwf.platform_fk
        , cwf.certificate_type_fk
        , cwf.platform
        , cwf.certificate_scope
        , cwf.certificate_uuid
        , cwf.certificate_is_revoked
        , cwf.certificate_created_on
        , cwf.certificate_updated_on
        , cwf.certificate_issued_on
        , cwf._cross_source_row_num = 1 as is_current
    from cross_source_deduped as cwf

    {% if is_incremental() %}
    -- left join preserves certificates from platforms not yet in the target table
    left join incremental_watermarks w
        on w.watermark_platform = cwf.platform
        and w.watermark_certificate_type = cwf.certificate_scope
    left join stale_user_fk_lookup as sufk
        on sufk.certificate_key = {{ dbt_utils.generate_surrogate_key([
            "cast(cwf.certificate_id as varchar)",
            "cwf.platform",
            "cwf.certificate_scope"
        ]) }}
    where (
        w.max_activity_on is null  -- platform/type not yet in target, include all
        or coalesce(cwf.certificate_updated_on, cwf.certificate_created_on) >= w.max_activity_on
        or cwf.certificate_created_on is null
        -- dim_user re-key: re-select rows whose resolved user_fk no longer matches the target
        or sufk.stored_user_fk is distinct from cwf.user_fk
        -- A newer certificate can take is_current from a row whose own timestamps did
        -- not move, so re-select any row whose flag no longer matches the target.
        or sufk.stored_is_current is distinct from (cwf._cross_source_row_num = 1)
    )

    -- A certificate that has left the source can never be re-selected above, so it would
    -- stay current forever. Carry it forward from the target as not current. This is also
    -- what retires a micromasters copy loaded before its edxorg record resolved.
    union all

    select
        target.certificate_key
        , target.certificate_id
        , target.certificate_issued_date_key
        , target.user_fk
        , target.courserun_fk
        , target.program_fk
        , target.platform_fk
        , target.certificate_type_fk
        , target.platform
        , target.certificate_scope
        , target.certificate_uuid
        , target.certificate_is_revoked
        , target.certificate_created_on
        , target.certificate_updated_on
        , target.certificate_issued_on
        , false as is_current
    from {{ this }} as target
    inner join source_platforms
        on target.platform = source_platforms.platform
        and target.certificate_scope = source_platforms.certificate_scope
    left join cross_source_deduped as source_certificates
        on target.certificate_key = {{ dbt_utils.generate_surrogate_key([
            "cast(source_certificates.certificate_id as varchar)",
            "source_certificates.platform",
            "source_certificates.certificate_scope"
        ]) }}
    where source_certificates.certificate_id is null
    {% if target_has_is_current %}
    and target.is_current is distinct from false
    {% endif %}
    {% endif %}
)

-- Defensive dedup: certificates_keyed already leaves one row per certificate_key, and a
-- carried-forward row has no source row by construction. This guard keeps a duplicate key
-- out of the incremental delete+insert if either stops holding.
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
        , is_current
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
    , is_current
from final_deduped
where _row_num = 1
