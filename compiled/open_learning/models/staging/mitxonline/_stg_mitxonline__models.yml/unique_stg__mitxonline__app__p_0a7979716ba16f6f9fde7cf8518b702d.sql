select
    programcertificate_uuid as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxonline__app__postgres__courses_programcertificate
where programcertificate_uuid is not null
group by programcertificate_uuid
having count(*) > 1
