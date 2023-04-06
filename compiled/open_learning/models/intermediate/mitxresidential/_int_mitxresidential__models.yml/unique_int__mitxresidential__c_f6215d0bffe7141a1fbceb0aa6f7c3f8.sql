select
    courserunenrollment_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxresidential__courserun_enrollments
where courserunenrollment_id is not null
group by courserunenrollment_id
having count(*) > 1
