with child as (
    select courserun_id as from_field
    from dev.main_intermediate.int__bootcamps__courserun_certificates
    where courserun_id is not null
)

, parent as (
    select courserun_id as to_field
    from dev.main_intermediate.int__bootcamps__course_runs
)

select from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
