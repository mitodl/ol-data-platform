{{ config(
    materialized='incremental',
    unique_key='courserun_pk',
    on_schema_change='append_new_columns'
) }}

with mitxonline_courseruns as (
    select
        courserun_readable_id
        , courserun_id as source_id
        , course_id
        , courserun_title
        , courserun_start_on
        , courserun_end_on
        , courserun_enrollment_start_on as enrollment_start
        , courserun_enrollment_end_on as enrollment_end
        , courserun_is_live
        , '{{ var("mitxonline") }}' as platform
    from {{ ref('int__mitxonline__course_runs') }}
)

, mitxpro_courseruns as (
    select
        courserun_readable_id
        , courserun_id as source_id
        , course_id
        , courserun_title
        , courserun_start_on
        , courserun_end_on
        , courserun_enrollment_start_on as enrollment_start
        , courserun_enrollment_end_on as enrollment_end
        , courserun_is_live
        , '{{ var("mitxpro") }}' as platform
    from {{ ref('int__mitxpro__course_runs') }}
)

, edxorg_courseruns as (
    select
        courserun_readable_id
        , null as source_id
        , null as course_id
        , courserun_title
        , courserun_start_on
        , courserun_end_on
        , null as enrollment_start
        , null as enrollment_end
        , true as courserun_is_live
        , '{{ var("edxorg") }}' as platform
    from {{ ref('int__edxorg__mitx_courseruns') }}
)

, combined_courseruns as (
    select * from mitxonline_courseruns
    union all
    select * from mitxpro_courseruns
    union all
    select * from edxorg_courseruns
)

-- Join to dim_course to get course_fk
, dim_course as (
    select
        course_pk
        , course_readable_id
    from {{ ref('dim_course') }}
    where is_current = true
)

, courseruns_with_fk as (
    select
        combined_courseruns.*
        , dim_course.course_pk as course_fk
    from combined_courseruns
    left join dim_course
        on
            -- Extract course ID from course run ID
            case
                when combined_courseruns.courserun_readable_id like 'course-v1:%'
                    then substring(
                        combined_courseruns.courserun_readable_id,
                        1,
                        regexp_position(combined_courseruns.courserun_readable_id, '\+type@', 1) - 1
                    )
                else combined_courseruns.courserun_readable_id
            end = dim_course.course_readable_id
)

, dim_platform as (
    select
        platform_pk
        , platform_readable_id
    from {{ ref('dim_platform') }}
)

, courseruns_with_all_fks as (
    select
        courseruns_with_fk.*
        , dim_platform.platform_pk as platform_fk
        -- Create date keys for date dimension joins
        , cast(format_datetime(courserun_start_on, 'yyyyMMdd') as integer) as start_date_key
        , cast(format_datetime(courserun_end_on, 'yyyyMMdd') as integer) as end_date_key
        , cast(format_datetime(enrollment_start, 'yyyyMMdd') as integer) as enrollment_start_date_key
        , cast(format_datetime(enrollment_end, 'yyyyMMdd') as integer) as enrollment_end_date_key
    from courseruns_with_fk
    left join dim_platform on courseruns_with_fk.platform = dim_platform.platform_readable_id
)

, final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'courserun_readable_id',
            'cast(current_timestamp as varchar)'
        ]) }} as courserun_pk
        , courserun_readable_id
        , course_fk
        , platform_fk
        , courserun_title
        , start_date_key
        , end_date_key
        , enrollment_start_date_key
        , enrollment_end_date_key
        , courserun_start_on
        , courserun_end_on
        , enrollment_start
        , enrollment_end
        , courserun_is_live
        , current_timestamp as effective_date
        , cast(null as timestamp) as end_date
        , true as is_current
    from courseruns_with_all_fks

    {% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} as existing
        where
            existing.courserun_readable_id = courseruns_with_all_fks.courserun_readable_id
            and existing.is_current = true
            and existing.courserun_title = courseruns_with_all_fks.courserun_title
    )
    {% endif %}
)

select * from final
