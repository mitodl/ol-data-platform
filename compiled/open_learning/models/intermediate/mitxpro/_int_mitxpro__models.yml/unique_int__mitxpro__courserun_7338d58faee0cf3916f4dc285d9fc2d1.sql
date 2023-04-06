select
    courseruncertificate_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxpro__courserun_certificates
where courseruncertificate_id is not null
group by courseruncertificate_id
having count(*) > 1
