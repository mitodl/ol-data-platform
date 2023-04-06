with child as (
    select opportunity_id as from_field
    from dev.main_staging.stg__salesforce__opportunitylineitem
    where opportunity_id is not null
)

, parent as (
    select opportunity_id as to_field
    from dev.main_staging.stg__salesforce__opportunity
)

select from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
