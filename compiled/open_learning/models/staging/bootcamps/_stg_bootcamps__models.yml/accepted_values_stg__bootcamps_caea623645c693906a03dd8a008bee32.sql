with all_values as (

    select
        courserunenrollment_enrollment_status as value_field
        , count(*) as n_records

    from dev.main_staging.stg__bootcamps__app__postgres__courserunenrollment
    group by courserunenrollment_enrollment_status

)

select *
from all_values
where value_field not in (
    'deferred', 'refunded', 'None'
)
