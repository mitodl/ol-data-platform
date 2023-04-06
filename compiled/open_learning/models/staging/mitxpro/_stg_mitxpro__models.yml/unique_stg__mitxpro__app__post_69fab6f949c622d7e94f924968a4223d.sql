select
    courseruncertificate_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxpro__app__postgres__courses_courseruncertificate
where courseruncertificate_id is not null
group by courseruncertificate_id
having count(*) > 1
