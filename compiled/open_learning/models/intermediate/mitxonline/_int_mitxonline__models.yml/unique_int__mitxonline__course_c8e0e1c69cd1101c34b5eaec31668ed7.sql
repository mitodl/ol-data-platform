select
    courseruncertificate_uuid as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxonline__courserun_certificates
where courseruncertificate_uuid is not null
group by courseruncertificate_uuid
having count(*) > 1
