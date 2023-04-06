select
    programcertificate_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__micromasters__app__postgres__grades_programcertificate
where programcertificate_id is not null
group by programcertificate_id
having count(*) > 1
