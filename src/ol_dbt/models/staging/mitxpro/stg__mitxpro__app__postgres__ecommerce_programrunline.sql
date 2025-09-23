with
    source as (

        select * from {{ source("ol_warehouse_raw_data", "raw__xpro__app__postgres__ecommerce_programrunline") }}

    ),
    renamed as (

        select
            id as programrunline_id,
            line_id,
            program_run_id as programrun_id,
            {{ cast_timestamp_to_iso8601("created_on") }} as programrunline_created_on,
            {{ cast_timestamp_to_iso8601("updated_on") }} as programrunline_updated_on
        from source

    )

select *
from renamed
