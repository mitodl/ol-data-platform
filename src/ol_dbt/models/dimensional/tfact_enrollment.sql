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
        row_number() over (order by user_id, courserunenrollment_created_on) as courserunenrollment_id  -- synthetic ID since edxorg doesn't have enrollment_id
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
-- dim_user not in Phase 1-2

, dim_course_run as (
    select courserun_pk, source_id, platform_fk, platform
    from {{ ref('dim_course_run') }}
    where is_current = true
)

, dim_program as (
    select program_pk, source_id, platform_fk, platform_readable_id as platform
    from {{ ref('dim_program') }}
)

-- dim_platform not in Phase 1-2

, enrollments_with_fks as (
    select
        combined_enrollments.*
        , cast(null as varchar) as user_fk  -- dim_user not in Phase 1-2
        , dim_course_run.courserun_pk as courserun_fk
        , dim_program.program_pk as program_fk
        , cast(null as integer) as platform_fk  -- dim_platform not in Phase 1-2
        , {{ iso8601_to_date_key('enrollment_created_on') }} as enrollment_date_key
    from combined_enrollments
    left join dim_course_run
        on combined_enrollments.courserun_id = dim_course_run.source_id
        and combined_enrollments.platform = dim_course_run.platform
    left join dim_program
        on combined_enrollments.program_id = dim_program.source_id
        and combined_enrollments.platform = dim_program.platform
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
        , platform
        , enrollment_is_active
        , enrollment_mode
        , enrollment_status
        , enrollment_created_on
    from enrollments_with_fks

    {% if is_incremental() %}
    where enrollment_created_on > (select max(enrollment_created_on) from {{ this }})
    {% endif %}
)

select * from final
