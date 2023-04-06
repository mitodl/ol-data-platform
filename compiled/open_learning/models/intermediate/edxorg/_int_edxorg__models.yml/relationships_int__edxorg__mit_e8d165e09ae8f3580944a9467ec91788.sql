with child as (
    select user_id as from_field
    from dev.main_intermediate.int__edxorg__mitx_courserun_grades
    where user_id is not null
)

, parent as (
    select user_id as to_field
    from dev.main_intermediate.int__edxorg__mitx_users
)

select from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
