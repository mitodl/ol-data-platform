with child as (
    select program_id as from_field
    from dev.main_intermediate.int__mitxonline__program_to_courses
    where program_id is not null
)

, parent as (
    select program_id as to_field
    from dev.main_intermediate.int__mitxonline__programs
)

select from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
