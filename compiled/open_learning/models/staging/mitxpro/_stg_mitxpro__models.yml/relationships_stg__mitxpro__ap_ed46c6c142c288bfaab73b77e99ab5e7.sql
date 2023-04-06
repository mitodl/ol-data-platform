with child as (
    select program_id as from_field
    from dev.main_staging.stg__mitxpro__app__postgres__courses_programcertificate
    where program_id is not null
)

, parent as (
    select program_id as to_field
    from dev.main_staging.stg__mitxpro__app__postgres__courses_program
)

select from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
