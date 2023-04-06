select
    programcertificate_id as unique_field
    , count(*) as n_records

from ol_data_lake_production.ol_warehouse_production_intermediate.int__micromasters__program_certificates
where programcertificate_id is not null
group by programcertificate_id
having count(*) > 1
