select
    courseruncertificate_uuid as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxonline__app__postgres__courses_courseruncertificate
where courseruncertificate_uuid is not null
group by courseruncertificate_uuid
having count(*) > 1
