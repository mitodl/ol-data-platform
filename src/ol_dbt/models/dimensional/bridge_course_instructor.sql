{{ config(
    materialized='table'
) }}

-- Map courses to instructors (many-to-many)
with mitxonline_course_instructors as (
    select
        course_id
        , instructor_name
        , 'mitxonline' as platform
    from {{ ref('int__mitxonline__course_instructors') }}
)

, mitxpro_course_instructors as (
    select
        course_id
        , cms_facultymemberspage_facultymember_name as instructor_name
        , 'mitxpro' as platform
    from {{ ref('int__mitxpro__coursesfaculty') }}
)

, source_id_course_instructors as (
    select * from mitxonline_course_instructors
    union all
    select * from mitxpro_course_instructors
)

-- OCW source_id is null in dim_course; join on course_readable_id
, ocw_course_instructors as (
    select
        ocw_courses.course_readable_id
        , ocw_instructors.course_instructor_title as instructor_name
        , 'ocw' as platform
    from {{ ref('int__ocw__course_instructors') }} as ocw_instructors
    inner join {{ ref('int__ocw__courses') }} as ocw_courses
        on ocw_instructors.course_uuid = ocw_courses.course_uuid
)

-- Join to dimensions to get FKs
, dim_course as (
    select
        course_pk
        , course_readable_id
        , source_id
        , primary_platform
    from {{ ref('dim_course') }}
    where is_current = true
)

, dim_instructor as (
    select
        instructor_pk
        , instructor_name
        , primary_platform
    from {{ ref('dim_instructor') }}
)

, source_id_bridge as (
    select
        dim_course.course_pk as course_fk
        , dim_instructor.instructor_pk as instructor_fk
    from source_id_course_instructors
    left join dim_course
        on source_id_course_instructors.course_id = dim_course.source_id
        and source_id_course_instructors.platform = dim_course.primary_platform
    left join dim_instructor
        on source_id_course_instructors.instructor_name = dim_instructor.instructor_name
        and source_id_course_instructors.platform = dim_instructor.primary_platform
)

, ocw_bridge as (
    select
        dim_course.course_pk as course_fk
        , dim_instructor.instructor_pk as instructor_fk
    from ocw_course_instructors
    left join dim_course
        on ocw_course_instructors.course_readable_id = dim_course.course_readable_id
        and ocw_course_instructors.platform = dim_course.primary_platform
    left join dim_instructor
        on ocw_course_instructors.instructor_name = dim_instructor.instructor_name
        and ocw_course_instructors.platform = dim_instructor.primary_platform
)

, bridge as (
    select * from source_id_bridge
    union all
    select * from ocw_bridge
)

-- Include only fully-resolved rows; unresolved FKs indicate dim coverage gaps
select distinct
    course_fk
    , instructor_fk
from bridge
where course_fk is not null
    and instructor_fk is not null
