{{ config(
    materialized='incremental',
    unique_key='certificate_key',
    on_schema_change='append_new_columns'
) }}

-- Consolidate certificates from all platforms
with mitxonline_certificates as (
    select
        courseruncertificate_id as certificate_id
        , user_id
        , courserun_id
        , courseruncertificate_uuid as certificate_uuid
        , courseruncertificate_is_revoked as certificate_is_revoked
        , courseruncertificate_created_on as certificate_created_on
        , '{{ var("mitxonline") }}' as platform
    from {{ ref('int__mitxonline__courserun_certificates') }}
)

, mitxpro_certificates as (
    select
        courseruncertificate_id as certificate_id
        , user_id
        , courserun_id
        , courseruncertificate_uuid as certificate_uuid
        , courseruncertificate_is_revoked as certificate_is_revoked
        , courseruncertificate_created_on as certificate_created_on
        , '{{ var("mitxpro") }}' as platform
    from {{ ref('int__mitxpro__courserun_certificates') }}
)

-- edxorg_certificates omitted - requires complex intermediate dependencies

, combined_certificates as (
    select * from mitxonline_certificates
    union all
    select * from mitxpro_certificates
)

-- dim_user not in Phase 1-2 (not joined)

, dim_course_run as (
    select courserun_pk, source_id, platform
    from {{ ref('dim_course_run') }}
    where is_current = true
)

-- dim_platform not in Phase 1-2

, certificates_with_fks as (
    select
        combined_certificates.*
        , cast(null as varchar) as user_fk  -- dim_user not in Phase 1-2
        , dim_course_run.courserun_pk as courserun_fk
        , cast(null as integer) as platform_fk  -- dim_platform not in Phase 1-2
        , {{ iso8601_to_date_key('certificate_created_on') }} as certificate_date_key
    from combined_certificates
    left join dim_course_run
        on combined_certificates.courserun_id = dim_course_run.source_id
        and combined_certificates.platform = dim_course_run.platform
)

, final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cast(certificate_id as varchar)',
            'platform'
        ]) }} as certificate_key
        , certificate_id
        , certificate_date_key
        , user_fk
        , courserun_fk
        , platform_fk
        , platform
        , certificate_uuid
        , certificate_is_revoked
        , certificate_created_on
    from certificates_with_fks

    {% if is_incremental() %}
    where certificate_created_on >= (select max(certificate_created_on) from {{ this }})
    {% endif %}
)

select * from final
