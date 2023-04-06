select
    courserunenrollment_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxonline__app__postgres__courses_courserunenrollment
where courserunenrollment_id is not null
group by courserunenrollment_id
having count(*) > 1
