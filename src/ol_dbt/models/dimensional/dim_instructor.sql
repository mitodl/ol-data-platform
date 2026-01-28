{{ config(
    materialized='table'
) }}

-- Consolidate instructors from all platforms
with mitxonline_instructors as (
    select
        instructor_name
        , instructor_title
        , instructor_bio_short
        , instructor_bio_long
        , '{{ var("mitxonline") }}' as platform
    from {{ ref('int__mitxonline__course_instructors') }}
)

, mitxpro_instructors as (
    select
        cms_facultymemberspage_facultymember_name as instructor_name
        , cast(null as varchar) as instructor_title
        , cast(null as varchar) as instructor_bio_short
        , cms_facultymemberspage_facultymember_description as instructor_bio_long
        , '{{ var("mitxpro") }}' as platform
    from {{ ref('int__mitxpro__coursesfaculty') }}
)

, ocw_instructors as (
    select
        course_instructor_title as instructor_name
        , course_instructor_salutation as instructor_title
        , cast(null as varchar) as instructor_bio_short
        , cast(null as varchar) as instructor_bio_long
        , 'ocw' as platform
    from ol_data_lake_production.ol_warehouse_production_intermediate.int__ocw__course_instructors
)

, combined_instructors as (
    select * from mitxonline_instructors
    union all
    select * from mitxpro_instructors
    union all
    select * from ocw_instructors
)

-- Deduplicate by instructor_name (same person can teach on multiple platforms)
, deduped_instructors as (
    select
        instructor_name
        , max(instructor_title) as instructor_title
        , max(instructor_bio_short) as instructor_bio_short
        , max(instructor_bio_long) as instructor_bio_long
        , min(platform) as primary_platform
    from combined_instructors
    where instructor_name is not null
    group by instructor_name
)

select
    {{ dbt_utils.generate_surrogate_key(['instructor_name']) }} as instructor_pk
    , instructor_name
    , instructor_title
    , instructor_bio_short
    , instructor_bio_long
    , primary_platform
from deduped_instructors
