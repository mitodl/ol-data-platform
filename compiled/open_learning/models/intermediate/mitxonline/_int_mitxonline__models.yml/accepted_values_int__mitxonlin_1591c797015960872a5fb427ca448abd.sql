with all_values as (

    select
        courserunenrollment_enrollment_status as value_field
        , count(*) as n_records

    from dev.main_intermediate.int__mitxonline__courserunenrollments
    group by courserunenrollment_enrollment_status

)

select *
from all_values
where value_field not in (
    'deferred', 'transferred', 'refunded', 'enrolled', 'unenrolled', ''
)
