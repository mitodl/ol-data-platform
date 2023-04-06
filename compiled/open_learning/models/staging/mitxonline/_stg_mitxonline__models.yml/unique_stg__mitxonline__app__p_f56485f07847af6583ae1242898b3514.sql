select
    programenrollment_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxonline__app__postgres__courses_programenrollment
where programenrollment_id is not null
group by programenrollment_id
having count(*) > 1
