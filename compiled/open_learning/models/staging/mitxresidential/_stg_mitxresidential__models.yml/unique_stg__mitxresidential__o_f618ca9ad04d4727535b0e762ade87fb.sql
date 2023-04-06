select
    courserunenrollment_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxresidential__openedx__courserun_enrollment
where courserunenrollment_id is not null
group by courserunenrollment_id
having count(*) > 1
