{{ config(
    materialized='table'
) }}

-- Map courses to instructors (many-to-many)
-- Note: OCW courses not yet in dim_course (Phase 1-2), so omitted here
with mitxonline_course_instructors as (
    select
        course_id
        , instructor_name
        , '{{ var("mitxonline") }}' as platform
    from {{ ref('int__mitxonline__course_instructors') }}
)

, mitxpro_course_instructors as (
    select
        course_id
        , cms_facultymemberspage_facultymember_name as instructor_name
        , '{{ var("mitxpro") }}' as platform
    from {{ ref('int__mitxpro__coursesfaculty') }}
)

, combined_course_instructors as (
    select * from mitxonline_course_instructors
    union all
    select * from mitxpro_course_instructors
)

-- Join to dimensions to get FKs
, dim_course as (
    select course_pk, source_id, primary_platform
    from {{ ref('dim_course') }}
    where is_current = true
)

, dim_instructor as (
    select instructor_pk, instructor_name
    from {{ ref('dim_instructor') }}
)

, bridge as (
    select
        dim_course.course_pk as course_fk
        , dim_instructor.instructor_pk as instructor_fk
    from combined_course_instructors
    inner join dim_course
        on combined_course_instructors.course_id = dim_course.source_id
        and combined_course_instructors.platform = dim_course.primary_platform
    inner join dim_instructor
        on combined_course_instructors.instructor_name = dim_instructor.instructor_name
)

-- Deduplicate in case same instructor-course pair exists multiple times
select distinct
    course_fk
    , instructor_fk
from bridge
