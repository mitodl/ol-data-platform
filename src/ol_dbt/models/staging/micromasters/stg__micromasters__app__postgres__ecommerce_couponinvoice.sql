with
    source as (

        select * from {{ source("ol_warehouse_raw_data", "raw__micromasters__app__postgres__ecommerce_couponinvoice") }}

    ),
    renamed as (

        select id as couponinvoice_id, invoice_number as couponinvoice_number, description as couponinvoice_description
        from source

    )

select *
from renamed
