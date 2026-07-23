{{ config(
    materialized='table'
) }}

-- Consolidate instructors from all platforms
with mitxonline_instructors as (
    select
        cast(instructor_source_id as varchar) as instructor_source_id
        , instructor_name
        , instructor_title
        , instructor_bio_short
        , instructor_bio_long
        , 'mitxonline' as platform
    from {{ ref('int__mitxonline__course_instructors') }}
)

, mitxpro_instructors as (
    select
        -- mitxpro's CMS faculty entries carry no stable per-instructor id (only a
        -- name and description embedded in a JSON array), so this platform must
        -- keep deduping on name alone and accepts the risk that two distinct
        -- faculty sharing an exact name on the same course page will collapse
        -- into a single dim_instructor row. See _course_catalog_dimensions.yml.
        cast(null as varchar) as instructor_source_id
        , cms_facultymemberspage_facultymember_name as instructor_name
        , cast(null as varchar) as instructor_title
        , cast(null as varchar) as instructor_bio_short
        , cms_facultymemberspage_facultymember_description as instructor_bio_long
        , 'mitxpro' as platform
    from {{ ref('int__mitxpro__coursesfaculty') }}
)

, ocw_instructors as (
    select
        cast(course_instructor_uuid as varchar) as instructor_source_id
        , course_instructor_title as instructor_name
        , course_instructor_salutation as instructor_title
        , cast(null as varchar) as instructor_bio_short
        , cast(null as varchar) as instructor_bio_long
        , 'ocw' as platform
    from {{ ref('int__ocw__course_instructors') }}
)

, combined_instructors as (
    select * from mitxonline_instructors
    union all
    select * from mitxpro_instructors
    union all
    select * from ocw_instructors
)

-- Keep per-platform rows — same name on different platforms are distinct instructors.
-- Dedupe on the platform's stable natural key (wagtail_page_id for mitxonline,
-- course_instructor_uuid for ocw) when available, falling back to instructor_name
-- for mitxpro, which has no such key. This prevents two different instructors who
-- happen to share a name on the same platform from silently collapsing into one row.
, deduped_instructors as (
    select
        coalesce(instructor_source_id, instructor_name) as instructor_dedup_key
        , max(instructor_source_id) as instructor_source_id
        , max(instructor_name) as instructor_name
        , max(instructor_title) as instructor_title
        , max(instructor_bio_short) as instructor_bio_short
        , max(instructor_bio_long) as instructor_bio_long
        , platform as primary_platform
    from combined_instructors
    where instructor_name is not null
    group by coalesce(instructor_source_id, instructor_name), platform
)

select
    {{ dbt_utils.generate_surrogate_key(['instructor_dedup_key', 'primary_platform']) }} as instructor_pk
    , instructor_source_id
    , instructor_name
    , instructor_title
    , instructor_bio_short
    , instructor_bio_long
    , primary_platform
from deduped_instructors
