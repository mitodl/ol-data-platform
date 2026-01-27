{{ config(
    materialized='incremental',
    unique_key='course_pk',
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
        , '{{ var("mitxonline") }}' as platform
    from {{ ref('int__mitxonline__courses') }}
)

, mitxpro_courses as (
    select
        course_readable_id
        , course_id as source_id
        , course_title
        , course_number
        , course_description
        , course_is_live
        , '{{ var("mitxpro") }}' as platform
    from {{ ref('int__mitxpro__courses') }}
)

, ocw_courses as (
    select
        concat('ocw-', cast(course_id as varchar)) as course_readable_id
        , course_id as source_id
        , course_title
        , course_number
        , course_description
        , true as course_is_live
        , '{{ var("ocw") }}' as platform
    from {{ ref('int__ocw__courses') }}
)

, combined_courses as (
    select * from mitxonline_courses
    union all
    select * from mitxpro_courses
    union all
    select * from ocw_courses
)

-- Deduplicate by course_readable_id (prefer most recent platform)
, deduped_courses as (
    select
        *
        , row_number() over (
            partition by course_readable_id
            order by
                case platform
                    when '{{ var("mitxonline") }}' then 1
                    when '{{ var("mitxpro") }}' then 2
                    when '{{ var("ocw") }}' then 3
                end
        ) as row_num
    from combined_courses
)

, current_courses as (
    select * from deduped_courses where row_num = 1
)

-- SCD Type 2 logic: Detect changes
, new_and_changed_courses as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'course_readable_id',
            'cast(current_timestamp as varchar)'
        ]) }} as course_pk
        , course_readable_id
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
            and existing.is_current = true
            and existing.course_title = current_courses.course_title
            and coalesce(existing.course_description, '') = coalesce(current_courses.course_description, '')
    )
    {% endif %}
)

{% if is_incremental() %}
-- Update existing records: Set end_date and is_current for changed records
, records_to_expire as (
    select
        existing.course_pk
        , existing.course_readable_id
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
