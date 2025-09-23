with
    source as (
        select *
        from {{ source("ol_warehouse_raw_data", "raw__edxorg__s3__course_structure__course_certificate_signatory") }}
    )

    {{ deduplicate_raw_table(order_by="_airbyte_extracted_at", partition_columns="id, course_id") }},
    cleaned as (

        select
            id as signatory_id,
            course_id as courserun_readable_id,
            title as signatory_title,
            organization as signatory_organization,
            certificate_id as signatory_certificate_id,
            trim(name) as signatory_name,
            regexp_replace(trim(name), '^(Professor |Dr\. )', '') as signatory_normalized_name,
            concat('https://courses.edx.org', nullif(signature_image_path, '')) as signatory_image_url
        from most_recent_source
    )

select *
from cleaned
