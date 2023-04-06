with child as (
    select mitxpro_user_id as from_field
    from dev.main_intermediate.int__openedx_mitxpro__user_courseactivities
    where mitxpro_user_id is not null
)

, parent as (
    select user_id as to_field
    from dev.main_intermediate.int__mitxpro__users
)

select from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
