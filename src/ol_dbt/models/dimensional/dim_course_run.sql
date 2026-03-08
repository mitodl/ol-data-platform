{{ config(
    materialized='incremental',
    unique_key='courserun_pk',
    incremental_strategy='delete+insert',
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
        , 'mitxonline' as platform
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
        , 'mitxpro' as platform
    from {{ ref('int__mitxpro__course_runs') }}
)

, edxorg_courseruns as (
    select
        courserun_readable_id
        , cast(null as integer) as source_id
        , cast(null as integer) as course_id
        , courserun_title
        , courserun_start_date as courserun_start_on  -- edxorg uses _date suffix
        , courserun_end_date as courserun_end_on
        , courserun_enrollment_start_date as enrollment_start
        , courserun_enrollment_end_date as enrollment_end
        , courserun_is_published as courserun_is_live
        , 'edxorg' as platform
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
            -- course-v1 format: course-v1:{org}+{course}+{run} → strip the last +{run} segment
            case
                when combined_courseruns.courserun_readable_id like 'course-v1:%'
                    then substring(
                        combined_courseruns.courserun_readable_id,
                        1,
                        length(combined_courseruns.courserun_readable_id)
                        - strpos(reverse(combined_courseruns.courserun_readable_id), '+')
                    )
                else combined_courseruns.courserun_readable_id
            end = dim_course.course_readable_id
)

, courseruns_with_all_fks as (
    select
        courseruns_with_fk.*
        , cast(null as varchar) as platform_fk  -- dim_platform not in Phase 1-2
        -- Create date keys for date dimension joins (parse to timestamp then format as YYYYMMDD)
        , {{ iso8601_to_date_key('courserun_start_on') }} as start_date_key
        , {{ iso8601_to_date_key('courserun_end_on') }} as end_date_key
        , {{ iso8601_to_date_key('enrollment_start') }} as enrollment_start_date_key
        , {{ iso8601_to_date_key('enrollment_end') }} as enrollment_end_date_key
    from courseruns_with_fk
)

, final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'platform',
            'courserun_readable_id',
            'cast(current_timestamp as varchar)'
        ]) }} as courserun_pk
        , courserun_readable_id
        , source_id
        , course_fk
        , platform_fk
        , platform
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
            and existing.platform = courseruns_with_all_fks.platform
            and existing.is_current = true
            and existing.courserun_title = courseruns_with_all_fks.courserun_title
    )
    {% endif %}
)

{% if is_incremental() %}
-- Expire prior current rows that have changed
, records_to_expire as (
    select
        existing.courserun_pk
        , existing.courserun_readable_id
        , existing.source_id
        , existing.course_fk
        , existing.platform_fk
        , existing.platform
        , existing.courserun_title
        , existing.start_date_key
        , existing.end_date_key
        , existing.enrollment_start_date_key
        , existing.enrollment_end_date_key
        , existing.courserun_start_on
        , existing.courserun_end_on
        , existing.enrollment_start
        , existing.enrollment_end
        , existing.courserun_is_live
        , existing.effective_date
        , current_timestamp as end_date
        , false as is_current
    from {{ this }} as existing
    inner join final as new_records
        on existing.courserun_readable_id = new_records.courserun_readable_id
        and existing.platform = new_records.platform
    where existing.is_current = true
)

, combined as (
    select * from final
    union all
    select * from records_to_expire
)

select * from combined
{% else %}
select * from final
{% endif %}
