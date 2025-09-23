with
    source as (

        select * from {{ source("ol_warehouse_raw_data", "raw__ocw__studio__postgres__websites_websitestarter") }}

    ),
    renamed as (

        select
            id as websitestarter_id,
            name as websitestarter_name,
            slug as websitestarter_slug,
            source as websitestarter_source,
            path as websitestarter_path,
            status as websitestarter_status,
            {{ cast_timestamp_to_iso8601("created_on") }} as websitestarter_created_on,
            {{ cast_timestamp_to_iso8601("updated_on") }} as websitestarter_updated_on
        from source

    )

select *
from renamed
