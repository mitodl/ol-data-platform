{{ config(
    materialized='incremental',
    unique_key=['course_pk', 'effective_date'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Consolidate courses from all platforms
with mitxonline_courses as (
    select
        course_readable_id
        , course_id as source_id
        , course_title
        , course_number
        , course_description
        , course_is_live
        , 'mitxonline' as platform
    from {{ ref('int__mitxonline__courses') }}
)

, mitxpro_courses as (
    select
        course_readable_id
        , course_id as source_id
        , course_title
        -- Extract course_number from readable_id (format: course-v1:{org}+{course_number})
        , case
            when cardinality(split(course_readable_id, '+')) >= 2
                then split(course_readable_id, '+')[2]
        end as course_number
        , cast(null as varchar) as course_description  -- mitxpro doesn't have course_description
        , course_is_live
        , 'mitxpro' as platform
    from {{ ref('int__mitxpro__courses') }}
)

, edxorg_courses as (
    -- edxorg has no native course table — courses are represented only at the courserun level.
    -- One row per course_readable_id, using the most-recent course run to supply course-level
    -- attributes (title, course_number, is_live). source_id is always null for edxorg courses.
    -- Note: QUALIFY is not supported by Trino; using ROW_NUMBER subquery instead.
    select
        course_readable_id
        , source_id
        , course_title
        , course_number
        , course_description
        , course_is_live
        , platform
    from (
        select
            course_readable_id
            , cast(null as integer) as source_id
            , courserun_title as course_title
            , course_number
            , cast(null as varchar) as course_description
            , courserun_is_published as course_is_live
            , 'edxorg' as platform
            , row_number() over (
                partition by course_readable_id
                order by courserun_start_date desc nulls last
            ) as _row_num
        from {{ ref('int__edxorg__mitx_courseruns') }}
    )
    where _row_num = 1
)

, combined_courses as (
    select * from mitxonline_courses
    union all
    select * from mitxpro_courses
    union all
    select * from edxorg_courses
)

-- All (platform, course_readable_id) combinations are kept as distinct rows.
-- No cross-platform deduplication: the same course_readable_id on mitxonline and
-- mitxpro/edxorg produces separate rows, each with its own course_pk.
, current_courses as (
    select * from combined_courses
)

-- SCD Type 2 logic: Detect changes
, new_and_changed_courses as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'platform',
            'course_readable_id'
        ]) }} as course_pk
        , course_readable_id
        , source_id
        , course_number
        , course_title
        , course_description
        , course_is_live
        , platform as primary_platform
        , current_timestamp as effective_date
        , cast(null as timestamp) as end_date
        , true as is_current
    from current_courses

    {% if is_incremental() %}
    -- Only include new courses or courses with changed attributes
    where not exists (
        select 1
        from {{ this }} as existing
        where
            existing.course_readable_id = current_courses.course_readable_id
            and existing.primary_platform = current_courses.platform
            and existing.is_current = true
            and existing.course_title = current_courses.course_title
            and coalesce(existing.course_number, '') = coalesce(current_courses.course_number, '')
            and coalesce(existing.course_description, '') = coalesce(current_courses.course_description, '')
            and coalesce(existing.course_is_live, false) = coalesce(current_courses.course_is_live, false)
    )
    {% endif %}
)

{% if is_incremental() %}
-- Update existing records: Set end_date and is_current for changed records
, records_to_expire as (
    select
        existing.course_pk
        , existing.course_readable_id
        , existing.source_id
        , existing.course_number
        , existing.course_title
        , existing.course_description
        , existing.course_is_live
        , existing.primary_platform
        , existing.effective_date
        , current_timestamp as end_date
        , false as is_current
    from {{ this }} as existing
    inner join new_and_changed_courses as new_records
        on existing.course_readable_id = new_records.course_readable_id
        and existing.primary_platform = new_records.primary_platform
    where existing.is_current = true
)

, combined as (
    select * from new_and_changed_courses
    union all
    select * from records_to_expire
)

select * from combined
{% else %}
select * from new_and_changed_courses
{% endif %}
