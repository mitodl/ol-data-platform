with child as (
    select course_id as from_field
    from dev.main_intermediate.int__mitxonline__programrequirements
    where course_id is not null
)

, parent as (
    select course_id as to_field
    from dev.main_staging.stg__mitxonline__app__postgres__courses_course
)

select from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
