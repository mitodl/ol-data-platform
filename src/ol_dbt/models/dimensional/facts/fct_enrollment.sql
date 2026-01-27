{{ config(
    materialized='incremental',
    unique_key='enrollment_key',
    on_schema_change='append_new_columns'
) }}

-- Consolidate enrollments from all platforms
with mitxonline_enrollments as (
    select
        courserunenrollment_id as enrollment_id
        , user_id
        , courserun_id
        , null as program_id
        , courserunenrollment_created_on as enrollment_created_on
        , courserunenrollment_is_active as enrollment_is_active
        , courserunenrollment_enrollment_mode as enrollment_mode
        , courserunenrollment_enrollment_status as enrollment_status
        , '{{ var("mitxonline") }}' as platform
    from {{ ref('int__mitxonline__courserunenrollments') }}
)

, mitxpro_enrollments as (
    select
        courserunenrollment_id
        , user_id
        , courserun_id
        , null as program_id
        , courserunenrollment_created_on
        , courserunenrollment_is_active
        , courserunenrollment_enrollment_mode
        , null as enrollment_status
        , '{{ var("mitxpro") }}' as platform
    from {{ ref('int__mitxpro__courserunenrollments') }}
)

, edxorg_enrollments as (
    select
        courserunenrollment_id
        , user_id
        , null as courserun_id
        , null as program_id
        , courserunenrollment_created_on
        , courserunenrollment_is_active
        , courserunenrollment_enrollment_mode
        , null as enrollment_status
        , '{{ var("edxorg") }}' as platform
    from {{ ref('int__edxorg__mitx_courserun_enrollments') }}
)

, program_enrollments as (
    select
        programenrollment_id as enrollment_id
        , user_id
        , null as courserun_id
        , program_id
        , programenrollment_created_on as enrollment_created_on
        , programenrollment_is_active as enrollment_is_active
        , null as enrollment_mode
        , null as enrollment_status
        , '{{ var("mitxonline") }}' as platform
    from {{ ref('int__mitxonline__programenrollments') }}
)

, combined_enrollments as (
    select * from mitxonline_enrollments
    union all
    select * from mitxpro_enrollments
    union all
    select * from edxorg_enrollments
    union all
    select * from program_enrollments
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

, dim_program as (
    select program_pk, source_id, platform_fk
    from {{ ref('dim_program') }}
)

, dim_platform as (
    select platform_pk, platform_readable_id
    from {{ ref('dim_platform') }}
)

, enrollments_with_fks as (
    select
        combined_enrollments.*
        , dim_user.user_pk as user_fk
        , dim_course_run.courserun_pk as courserun_fk
        , dim_program.program_pk as program_fk
        , dim_platform.platform_pk as platform_fk
        , cast(format_datetime(enrollment_created_on, 'yyyyMMdd') as integer) as enrollment_date_key
    from combined_enrollments
    left join dim_platform on combined_enrollments.platform = dim_platform.platform_readable_id
    left join dim_user
        on
            (combined_enrollments.platform = '{{ var("mitxonline") }}' and combined_enrollments.user_id = dim_user.mitxonline_openedx_user_id)
            or (combined_enrollments.platform = '{{ var("mitxpro") }}' and combined_enrollments.user_id = dim_user.mitxpro_openedx_user_id)
            or (combined_enrollments.platform = '{{ var("edxorg") }}' and combined_enrollments.user_id = dim_user.edxorg_openedx_user_id)
    left join dim_course_run
        on combined_enrollments.courserun_id = dim_course_run.source_id
        and dim_platform.platform_pk = dim_course_run.platform_fk
    left join dim_program
        on combined_enrollments.program_id = dim_program.source_id
        and dim_platform.platform_pk = dim_program.platform_fk
)

, final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cast(enrollment_id as varchar)',
            'platform'
        ]) }} as enrollment_key
        , enrollment_id
        , enrollment_date_key
        , user_fk
        , courserun_fk
        , program_fk
        , platform_fk
        , enrollment_is_active
        , enrollment_mode
        , enrollment_status
    from enrollments_with_fks

    {% if is_incremental() %}
    where enrollment_created_on > (select max(enrollment_created_on) from {{ this }})
    {% endif %}
)

select * from final
