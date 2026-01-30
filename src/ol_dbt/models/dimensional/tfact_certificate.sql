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
--, edxorg_certificates as (
--    select
--        mitx_courseruncertificate_id as certificate_id
--        , user_id
--        , null as courserun_id
--        , mitx_courseruncertificate_uuid as certificate_uuid
--        , false as certificate_is_revoked
--        , mitx_courseruncertificate_created_on as certificate_created_on
--        , '{{ var("edxorg") }}' as platform
--    from {{ ref('int__edxorg__mitx_courserun_certificates') }}
--)

, combined_certificates as (
    select * from mitxonline_certificates
    union all
    select * from mitxpro_certificates
    -- union all
    -- select * from edxorg_certificates
)

-- dim_user not in Phase 1-2
--, dim_user as (
--    select user_pk, mitxonline_openedx_user_id, mitxpro_openedx_user_id, edxorg_openedx_user_id
--    from {{ ref('dim_user') }}
--)

, dim_course_run as (
    select courserun_pk, source_id, platform
    from {{ ref('dim_course_run') }}
    where is_current = true
)

-- dim_platform not in Phase 1-2
--, dim_platform as (
--    select platform_pk, platform_readable_id
--    from {{ ref('dim_platform') }}
--)

, certificates_with_fks as (
    select
        combined_certificates.*
        , cast(null as varchar) as user_fk  -- dim_user not in Phase 1-2
        , dim_course_run.courserun_pk as courserun_fk
        , cast(null as integer) as platform_fk  -- dim_platform not in Phase 1-2
        , case when certificate_created_on is not null
            then cast(date_format(
                case when length(certificate_created_on) = 10
                    then date_parse(certificate_created_on, '%Y-%m-%d')
                    else date_parse(substr(certificate_created_on, 1, 19), '%Y-%m-%dT%H:%i:%s')
                end, '%Y%m%d') as integer)
            else null end as certificate_date_key
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
    where certificate_created_on > (select max(certificate_created_on) from {{ this }})
    {% endif %}
)

select * from final
