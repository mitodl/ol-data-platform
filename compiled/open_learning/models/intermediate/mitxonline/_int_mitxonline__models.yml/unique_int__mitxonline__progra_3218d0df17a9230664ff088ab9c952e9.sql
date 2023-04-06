select
    programcertificate_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxonline__program_certificates
where programcertificate_id is not null
group by programcertificate_id
having count(*) > 1
