select
    course_readable_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxpro__courses
where course_readable_id is not null
group by course_readable_id
having count(*) > 1
