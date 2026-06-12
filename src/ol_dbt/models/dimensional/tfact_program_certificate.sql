{{ config(
    materialized='incremental',
    unique_key='program_certificate_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Consolidate program certificates from all platforms.
-- Grain: one row per (certificate_id, platform). Each row represents a single
-- program-level certificate earned by a learner.
--
-- Platforms covered: MITx Online, MITx Pro, edX.org (MicroMasters), MicroMasters.
-- Note: micromasters program certificates are sourced from int__micromasters__program_certificates
-- which applies DEDP-era logic (pre-2022-10-01 from MicroMasters DB, post from MITx Online).

with mitxonline_program_certificates as (
    select
        cast(programcertificate_id as varchar) as certificate_id
        , user_id
        , program_id
        , programcertificate_uuid as certificate_uuid
        , programcertificate_is_revoked as certificate_is_revoked
        , programcertificate_created_on as certificate_created_on
        , programcertificate_updated_on as certificate_updated_on
        , programcertificate_issued_on as certificate_issued_on
        , 'mitxonline' as platform
        , user_email
        , cast(program_id as varchar) as program_source_id
    from {{ ref('int__mitxonline__program_certificates') }}
)

, mitxpro_program_certificates as (
    select
        cast(programcertificate_id as varchar) as certificate_id
        , user_id
        , program_id
        , programcertificate_uuid as certificate_uuid
        , programcertificate_is_revoked as certificate_is_revoked
        , programcertificate_created_on as certificate_created_on
        , programcertificate_updated_on as certificate_updated_on
        , programcertificate_created_on as certificate_issued_on  -- mitxpro has no separate issued_on
        , 'mitxpro' as platform
        , user_email
        , cast(program_id as varchar) as program_source_id
    from {{ ref('int__mitxpro__program_certificates') }}
)

, edxorg_program_certificates as (
    select
        program_certificate_hashed_id as certificate_id
        , user_id
        , cast(null as integer) as program_id
        , program_certificate_hashed_id as certificate_uuid  -- no separate UUID; hashed ID doubles as identifier
        , false as certificate_is_revoked  -- edxorg doesn't track revocations
        , program_certificate_awarded_on as certificate_created_on
        , program_certificate_awarded_on as certificate_updated_on  -- only awarded_on available
        , program_certificate_awarded_on as certificate_issued_on
        , 'edxorg' as platform
        , cast(null as varchar) as user_email
        , program_uuid as program_source_id  -- edxorg uses UUID, not integer ID
    from {{ ref('int__edxorg__mitx_program_certificates') }}
)

-- MicroMasters program certificates use a complex DEDP-era union (pre/post 2022-10-01 cutoff).
-- These are sourced from micromasters DB for older certs and mitxonline DB for newer ones.
-- User resolution uses user_edxorg_id as the join key to dim_user.
, micromasters_program_certificates as (
    select
        program_certificate_hashed_id as certificate_id
        , cast(null as integer) as user_id  -- resolved via user_edxorg_id below
        , micromasters_program_id as program_id
        , cast(null as varchar) as certificate_uuid
        , false as certificate_is_revoked
        , program_completion_timestamp as certificate_created_on
        , program_completion_timestamp as certificate_updated_on
        , program_completion_timestamp as certificate_issued_on
        , 'micromasters' as platform
        , user_email
        , cast(micromasters_program_id as varchar) as program_source_id
        , user_edxorg_id
    from {{ ref('int__micromasters__program_certificates') }}
)

, combined as (
    select
        certificate_id
        , user_id
        , program_id
        , certificate_uuid
        , certificate_is_revoked
        , certificate_created_on
        , certificate_updated_on
        , certificate_issued_on
        , platform
        , user_email
        , program_source_id
        , cast(null as integer) as user_edxorg_id
    from mitxonline_program_certificates
    union all
    select
        certificate_id
        , user_id
        , program_id
        , certificate_uuid
        , certificate_is_revoked
        , certificate_created_on
        , certificate_updated_on
        , certificate_issued_on
        , platform
        , user_email
        , program_source_id
        , cast(null as integer) as user_edxorg_id
    from mitxpro_program_certificates
    union all
    select
        certificate_id
        , user_id
        , program_id
        , certificate_uuid
        , certificate_is_revoked
        , certificate_created_on
        , certificate_updated_on
        , certificate_issued_on
        , platform
        , user_email
        , program_source_id
        , cast(null as integer) as user_edxorg_id
    from edxorg_program_certificates
    union all
    select
        certificate_id
        , user_id
        , program_id
        , certificate_uuid
        , certificate_is_revoked
        , certificate_created_on
        , certificate_updated_on
        , certificate_issued_on
        , platform
        , user_email
        , program_source_id
        , user_edxorg_id
    from micromasters_program_certificates
)

, user_lookup as (
    select
        user_pk
        , email
        , mitxonline_application_user_id
        , mitxpro_application_user_id
        , edxorg_openedx_user_id
    from {{ ref('dim_user') }}
    where user_pk is not null
)

, dim_program as (
    select program_pk, source_id, platform_code
    from {{ ref('dim_program') }}
)

, dim_platform_lookup as (
    select platform_pk, platform_readable_id
    from {{ ref('dim_platform') }}
)

, combined_with_fks as (
    select
        combined.*
        , coalesce(
            case when combined.platform = 'mitxonline'
                then ul_mitxonline.user_pk
            end,
            case when combined.platform = 'mitxpro'
                then ul_mitxpro.user_pk
            end,
            case when combined.platform = 'edxorg'
                then ul_edxorg_userid.user_pk
            end,
            case when combined.platform = 'micromasters'
                then ul_micromasters_edxorg.user_pk
            end
        ) as user_fk
        , dim_program.program_pk as program_fk
        , dim_platform_lookup.platform_pk as platform_fk
        , {{ iso8601_to_date_key('certificate_issued_on') }} as certificate_issued_date_key
    from combined
    left join user_lookup as ul_mitxonline
        on combined.platform = 'mitxonline'
        and combined.user_id = ul_mitxonline.mitxonline_application_user_id
    left join user_lookup as ul_mitxpro
        on combined.platform = 'mitxpro'
        and combined.user_id = ul_mitxpro.mitxpro_application_user_id
    -- edxorg: join via openedx_user_id (integer user_id from source)
    left join user_lookup as ul_edxorg_userid
        on combined.platform = 'edxorg'
        and combined.user_id = ul_edxorg_userid.edxorg_openedx_user_id
    -- micromasters: join via edxorg_openedx_user_id (user_edxorg_id from source)
    left join user_lookup as ul_micromasters_edxorg
        on combined.platform = 'micromasters'
        and combined.user_edxorg_id = ul_micromasters_edxorg.edxorg_openedx_user_id
    left join dim_program
        on combined.program_source_id = dim_program.source_id
        and combined.platform = dim_program.platform_code
    left join dim_platform_lookup
        on combined.platform = dim_platform_lookup.platform_readable_id
)

{% if is_incremental() %}
-- Pre-compute per-platform watermarks in a single scan of {{ this }} to avoid
-- correlated subquery fan-out in Trino. Pattern mirrors tfact_certificate.sql.
, incremental_watermarks as (
    select
        platform as watermark_platform
        , max(coalesce(certificate_updated_on, certificate_created_on)) as max_activity_on
    from {{ this }}
    group by platform
)
{% endif %}

, final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cast(certificate_id as varchar)',
            'platform'
        ]) }} as program_certificate_key
        , certificate_id
        , certificate_issued_date_key
        , user_fk
        , program_fk
        , platform_fk
        , platform
        , certificate_uuid
        , certificate_is_revoked
        , certificate_created_on
        , certificate_updated_on
        , certificate_issued_on
    from combined_with_fks

    {% if is_incremental() %}
    left join incremental_watermarks w
        on w.watermark_platform = combined_with_fks.platform
    where (
        w.max_activity_on is null
        or coalesce(combined_with_fks.certificate_updated_on, combined_with_fks.certificate_created_on) >= w.max_activity_on
        or combined_with_fks.certificate_created_on is null
    )
    {% endif %}
)

-- Defensive dedup: guard against grain drift in upstream intermediates.
-- Note: QUALIFY is not supported by Trino; using ROW_NUMBER subquery instead.
, final_deduped as (
    select
        program_certificate_key
        , certificate_id
        , certificate_issued_date_key
        , user_fk
        , program_fk
        , platform_fk
        , platform
        , certificate_uuid
        , certificate_is_revoked
        , certificate_created_on
        , certificate_updated_on
        , certificate_issued_on
        , row_number() over (
            partition by program_certificate_key
            order by coalesce(certificate_updated_on, certificate_created_on) desc nulls last
        ) as _row_num
    from final
)

select
    program_certificate_key
    , certificate_id
    , certificate_issued_date_key
    , user_fk
    , program_fk
    , platform_fk
    , platform
    , certificate_uuid
    , certificate_is_revoked
    , certificate_created_on
    , certificate_updated_on
    , certificate_issued_on
from final_deduped
where _row_num = 1
