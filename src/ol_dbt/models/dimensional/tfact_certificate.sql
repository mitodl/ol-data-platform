{{ config(
    materialized='incremental',
    unique_key='certificate_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Consolidate certificates from all platforms
with mitxonline_certificates as (
    select
        courseruncertificate_id as certificate_id
        , user_id
        , courserun_id
        , cast(null as varchar) as courserun_readable_id  -- edxorg join key only
        , courseruncertificate_uuid as certificate_uuid
        , courseruncertificate_is_revoked as certificate_is_revoked
        , courseruncertificate_created_on as certificate_created_on
        , 'verified' as certificate_type_code
        , 'mitxonline' as platform
        , cast(null as varchar) as user_email  -- micromasters join key only
    from {{ ref('int__mitxonline__courserun_certificates') }}
)

, mitxpro_certificates as (
    select
        courseruncertificate_id as certificate_id
        , user_id
        , courserun_id
        , cast(null as varchar) as courserun_readable_id  -- edxorg join key only
        , courseruncertificate_uuid as certificate_uuid
        , courseruncertificate_is_revoked as certificate_is_revoked
        , courseruncertificate_created_on as certificate_created_on
        , 'professional' as certificate_type_code
        , 'mitxpro' as platform
        , cast(null as varchar) as user_email  -- micromasters join key only
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
        , courseruncertificate_mode as certificate_type_code
        , 'edxorg' as platform
        , cast(null as varchar) as user_email  -- micromasters join key only
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
        , 'honor' as certificate_type_code  -- MicroMasters edxorg certs are honor mode
        , 'micromasters' as platform
        , user_email
    from {{ ref('int__micromasters__course_certificates') }}
)

, combined_certificates as (
    select * from mitxonline_certificates
    union all
    select * from mitxpro_certificates
    union all
    select * from edxorg_certificates
    union all
    select * from micromasters_certificates
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

, dim_course_run as (
    select courserun_pk, source_id, courserun_readable_id, platform
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
            end
        ) as user_fk
        , dim_course_run.courserun_pk as courserun_fk
        , dim_platform_lookup.platform_pk as platform_fk
        , dim_certificate_type.certificate_type_pk as certificate_type_fk
        , {{ iso8601_to_date_key('certificate_created_on') }} as certificate_issued_date_key
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
        and combined_certificates.user_email = ul_micromasters.email
    left join dim_course_run
        on (
            (combined_certificates.platform in ('mitxonline', 'mitxpro')
                and combined_certificates.courserun_id = dim_course_run.source_id
                and combined_certificates.platform = dim_course_run.platform)
            or (combined_certificates.platform in ('edxorg', 'micromasters')
                and combined_certificates.courserun_readable_id = dim_course_run.courserun_readable_id)
        )
    left join dim_platform_lookup
        on combined_certificates.platform = dim_platform_lookup.platform_readable_id
    left join dim_certificate_type
        on dim_certificate_type.certificate_type_code = combined_certificates.certificate_type_code
)

, final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cast(certificate_id as varchar)',
            'platform'
        ]) }} as certificate_key
        , certificate_id
        , certificate_issued_date_key
        , user_fk
        , courserun_fk
        , platform_fk
        , certificate_type_fk
        , platform
        , certificate_uuid
        , certificate_is_revoked
        , certificate_created_on
    from certificates_with_fks

    {% if is_incremental() %}
    where certificate_created_on > (
            select max(certificate_created_on) from {{ this }}
            where platform = certificates_with_fks.platform
        )
        or certificate_created_on is null
    {% endif %}
)

-- Defensive dedup: the UNION ALL across 4 platform CTEs has no upstream uniqueness guarantee.
-- If any intermediate develops grain drift, this guard prevents duplicate certificate_key values
-- from silently entering the fact table and corrupting incremental MERGE operations.
select
    certificate_key
    , certificate_id
    , certificate_issued_date_key
    , user_fk
    , courserun_fk
    , platform_fk
    , certificate_type_fk
    , platform
    , certificate_uuid
    , certificate_is_revoked
    , certificate_created_on
from final
qualify row_number() over (
    partition by certificate_key
    order by certificate_created_on desc nulls last
) = 1
