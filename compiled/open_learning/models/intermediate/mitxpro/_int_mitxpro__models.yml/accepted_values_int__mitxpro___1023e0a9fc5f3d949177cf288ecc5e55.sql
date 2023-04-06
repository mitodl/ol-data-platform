with all_values as (

    select
        programenrollment_enrollment_status as value_field
        , count(*) as n_records

    from dev.main_intermediate.int__mitxpro__programenrollments
    group by programenrollment_enrollment_status

)

select *
from all_values
where value_field not in (
    'deferred', 'transferred', 'refunded', 'enrolled', 'unenrolled', '', 'None'
)
