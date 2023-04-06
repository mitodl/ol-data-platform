with child as (
    select micromasters_course_id as from_field
    from dev.main_intermediate.int__edxorg__mitx_courseruns
    where micromasters_course_id is not null
)

, parent as (
    select course_id as to_field
    from dev.main_staging.stg__micromasters__app__postgres__courses_course
)

select from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
