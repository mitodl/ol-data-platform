select
    coursetotopic_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxonline__app__postgres__courses_course_to_topic
where coursetotopic_id is not null
group by coursetotopic_id
having count(*) > 1
