select
    programcertificate_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxpro__app__postgres__courses_programcertificate
where programcertificate_id is not null
group by programcertificate_id
having count(*) > 1
