select
    course_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxonline__app__postgres__courses_course
where course_id is not null
group by course_id
having count(*) > 1
