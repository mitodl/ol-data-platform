with child as (
    select micromasters_program_id as from_field
    from dev.main_intermediate.int__edxorg__mitx_courseruns
    where micromasters_program_id is not null
)

, parent as (
    select program_id as to_field
    from dev.main_intermediate.int__micromasters__programs
)

select from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
