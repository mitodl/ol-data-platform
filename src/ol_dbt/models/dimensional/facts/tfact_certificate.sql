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

, edxorg_certificates as (
    select
        mitx_courseruncertificate_id as certificate_id
        , user_id
        , null as courserun_id
        , mitx_courseruncertificate_uuid as certificate_uuid
        , false as certificate_is_revoked
        , mitx_courseruncertificate_created_on as certificate_created_on
        , '{{ var("edxorg") }}' as platform
    from {{ ref('int__edxorg__mitx_courserun_certificates') }}
)

, combined_certificates as (
    select * from mitxonline_certificates
    union all
    select * from mitxpro_certificates
    union all
    select * from edxorg_certificates
)

-- Join to dimensions for FKs
, dim_user as (
    select user_pk, mitxonline_openedx_user_id, mitxpro_openedx_user_id, edxorg_openedx_user_id
    from {{ ref('dim_user') }}
)

, dim_course_run as (
    select courserun_pk, source_id, platform_fk
    from {{ ref('dim_course_run') }}
    where is_current = true
)

, dim_platform as (
    select platform_pk, platform_readable_id
    from {{ ref('dim_platform') }}
)

, certificates_with_fks as (
    select
        combined_certificates.*
        , dim_user.user_pk as user_fk
        , dim_course_run.courserun_pk as courserun_fk
        , dim_platform.platform_pk as platform_fk
        , cast(format_datetime(certificate_created_on, 'yyyyMMdd') as integer) as certificate_date_key
    from combined_certificates
    left join dim_platform on combined_certificates.platform = dim_platform.platform_readable_id
    left join dim_user
        on
            (combined_certificates.platform = '{{ var("mitxonline") }}' and combined_certificates.user_id = dim_user.mitxonline_openedx_user_id)
            or (combined_certificates.platform = '{{ var("mitxpro") }}' and combined_certificates.user_id = dim_user.mitxpro_openedx_user_id)
            or (combined_certificates.platform = '{{ var("edxorg") }}' and combined_certificates.user_id = dim_user.edxorg_openedx_user_id)
    left join dim_course_run
        on combined_certificates.courserun_id = dim_course_run.source_id
        and dim_platform.platform_pk = dim_course_run.platform_fk
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
        , certificate_uuid
        , certificate_is_revoked
    from certificates_with_fks

    {% if is_incremental() %}
    where certificate_created_on > (select max(certificate_created_on) from {{ this }})
    {% endif %}
)

select * from final
