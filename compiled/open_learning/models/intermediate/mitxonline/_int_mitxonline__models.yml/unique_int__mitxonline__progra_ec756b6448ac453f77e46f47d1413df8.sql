select
    programcertificate_uuid as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxonline__program_certificates
where programcertificate_uuid is not null
group by programcertificate_uuid
having count(*) > 1
