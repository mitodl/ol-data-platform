with child as (
    select course_id as from_field
    from dev.main_intermediate.int__mitxonline__courserun_grades
    where course_id is not null
)

, parent as (
    select course_id as to_field
    from dev.main_intermediate.int__mitxonline__courses
)

select from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
