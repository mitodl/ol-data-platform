with all_values as (

    select
        courserunenrollment_enrollment_status as value_field
        , count(*) as n_records

    from dev.main_staging.stg__mitxpro__app__postgres__courses_courserunenrollment
    group by courserunenrollment_enrollment_status

)

select *
from all_values
where value_field not in (
    'deferred', 'transferred', 'refunded', 'enrolled', 'unenrolled', '', 'None'
)
