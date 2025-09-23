with
    source as (
        select *
        from {{ source("ol_warehouse_raw_data", "raw__edxorg__s3__tables__certificates_generatedcertificate") }}
    )

    {{ deduplicate_raw_table(order_by="_airbyte_extracted_at", partition_columns="id") }},
    cleaned as (
        -- - all values are ingested as string, so we need to cast here to match other data sources
        select
            cast(id as integer) as courseruncertificate_id,
            course_id as courserun_readable_id,
            cast(user_id as integer) as user_id,
            name as courseruncertificate_user_full_name,
            key as courseruncertificate_key,
            mode as courseruncertificate_mode,
            download_url as courseruncertificate_download_url,
            download_uuid as courseruncertificate_download_uuid,
            verify_uuid as courseruncertificate_verify_uuid,
            status as courseruncertificate_status,
            try_cast(grade as decimal(38, 2)) as courseruncertificate_grade,
            case
                when lower(created_date) = 'null' or lower(created_date) = 'none'
                then null
                else to_iso8601(date_parse(created_date, '%Y-%m-%d %H:%i:%s'))
            end as courseruncertificate_created_on,
            case
                when lower(modified_date) = 'null' or lower(modified_date) = 'none'
                then null
                else to_iso8601(date_parse(modified_date, '%Y-%m-%d %H:%i:%s'))
            end as courseruncertificate_updated_on
        from most_recent_source
    )

select *
from cleaned
