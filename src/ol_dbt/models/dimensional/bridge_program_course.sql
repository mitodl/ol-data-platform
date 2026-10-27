-- Program requirements define which courses belong to which programs
with micromasters_course_keys as (
    select
        course_id
        , course_edx_key as course_readable_id
    from {{ ref('stg__micromasters__app__postgres__courses_course') }}
)

, mitxonline_program_requirements as (
    select
        cast(program_id as varchar) as program_id
        , course_id
        , cast(null as varchar) as course_readable_id
        -- An elective is one option in a "choose N of M" set, so it is not required on its
        -- own, nor is a course under an all_of nested in one. The number a program
        -- requires is program_num_required_courses upstream.
        , programrequirement_is_required as is_required
        , 'mitxonline' as platform
        , 'mitxonline' as platform_code
        , course_order_in_program as course_order
    from {{ ref('int__mitxonline__program_requirements') }}
)

, mitxpro_program_courses as (
    -- A course names its program in courses_course.program_id. The CMS program pages
    -- list fewer programs' courses, but add courses listed under a second program and
    -- the course order.
    select
        program_id
        , course_id
        , cast(null as bigint) as course_order
    from {{ ref('int__mitxpro__courses') }}
    where program_id is not null

    union all

    select
        program_id
        , course_id
        , course_order
    from {{ ref('int__mitxpro__coursesinprogram') }}
)

, mitxpro_program_requirements as (
    select
        cast(program_id as varchar) as program_id
        , course_id
        , cast(null as varchar) as course_readable_id
        -- xPRO programs have no elective sets: every course in one is required.
        , true as is_required
        , 'mitxpro' as platform
        , 'mitxpro' as platform_code
        , max(course_order) as course_order
    from mitxpro_program_courses
    group by program_id, course_id
)

, micromasters_requirements as (
    -- int__micromasters__program_requirements is at (program_id, course_id, electiveset_id)
    -- grain. A course can satisfy multiple elective sets within the same program, producing
    -- duplicate (program_id, course_readable_id) pairs. Since the bridge only models
    -- program→course membership (not which elective set), deduplicate here. A course is
    -- either Core or Elective in a program, never both, so the flag survives the distinct.
    select distinct
        cast(r.program_id as varchar) as program_id
        , cast(null as integer) as course_id  -- MicroMasters uses readable_id path
        , ck.course_readable_id
        , r.programrequirement_type = 'Core' as is_required
        , 'micromasters' as platform
        , 'micromasters' as platform_code
        -- position_in_program is a stable integer from the MicroMasters course table
        , r.course_position_in_program as course_order
    from {{ ref('int__micromasters__program_requirements') }} r
    inner join micromasters_course_keys ck on r.course_id = ck.course_id
)

, edxorg_program_requirements as (
    -- Preserve edX.org program memberships not already represented by a MITx Online
    -- program. Normalize the API course key to the MITx Online course key when the
    -- course moved there.
    select
        cast(program_courses.program_uuid as varchar) as program_id
        , cast(null as integer) as course_id
        , coalesce(mitx_courses.course_readable_id, program_courses.course_readable_id) as course_readable_id
        , true as is_required
        , 'edxorg' as platform
        , 'edxorg' as platform_code
        , cast(null as integer) as course_order
    from {{ ref('int__edxorg__mitx_program_courses') }} as program_courses
    -- A program that dim_program_report_key maps to a MITx Online program takes that
    -- program's courses, so its own edX.org memberships are left out.
    left join {{ ref('dim_program_report_key') }} as program_report_key
        on
            program_report_key.lookup_type = 'title'
            and program_report_key.is_mapped_to_mitxonline
            and program_courses.program_title = program_report_key.lookup_value
    left join {{ ref('int__mitx__courses') }} as mitx_courses
        on
            replace(program_courses.course_readable_id, 'MITx/', '')
            = replace(mitx_courses.course_readable_id, 'course-v1:MITxT+', '')
    where program_report_key.lookup_value is null
)

, combined_requirements as (
    select * from mitxonline_program_requirements
    union all
    select * from mitxpro_program_requirements
    union all
    select * from micromasters_requirements
    union all
    select * from edxorg_program_requirements
)

-- Join to dimensions to get FKs
, dim_program as (
    select program_pk, program_readable_id, source_id, platform_readable_id, platform_code
    from {{ ref('dim_program') }}
)

, dim_course as (
    select course_pk, course_readable_id, source_id, primary_platform
    from {{ ref('dim_course') }}
    where is_current = true
)

, bridge as (
    select
        dim_program.program_pk as program_fk
        , dim_course.course_pk as course_fk
        , combined_requirements.is_required
        , combined_requirements.course_order
    from combined_requirements
    left join dim_program
        on cast(combined_requirements.program_id as varchar) = dim_program.source_id
        and combined_requirements.platform_code = dim_program.platform_code
    left join dim_course
        on (
            -- Standard path: integer source_id + platform match
            (combined_requirements.platform_code != 'micromasters'
                and combined_requirements.course_id = dim_course.source_id
                and combined_requirements.platform_code = dim_course.primary_platform)
            -- MicroMasters path: courses run on edxorg; match on readable_id scoped to edxorg
            or (combined_requirements.platform_code = 'micromasters'
                and combined_requirements.course_readable_id = dim_course.course_readable_id
                and dim_course.primary_platform = 'edxorg')
            -- Other edX.org MITx programs use API course keys normalized above. A course
            -- in int__mitx__courses gets its MITx Online key, which dim_course holds under
            -- mitxonline, so the key alone identifies the course.
            or (combined_requirements.platform_code = 'edxorg'
                and combined_requirements.course_readable_id = dim_course.course_readable_id)
        )
)

select
    program_fk
    , course_fk
    , is_required
    , course_order
from bridge
where program_fk is not null
    and course_fk is not null
