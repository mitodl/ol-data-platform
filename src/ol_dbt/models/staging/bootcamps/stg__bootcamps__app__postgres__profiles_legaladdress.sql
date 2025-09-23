with
    source as (
        select * from {{ source("ol_warehouse_raw_data", "raw__bootcamps__app__postgres__profiles_legaladdress") }}
    ),
    cleaned as (

        select
            id as user_address_id,
            country as user_address_country,
            user_id,
            first_name as user_first_name,
            last_name as user_last_name,
            city as user_address_city,
            state_or_territory as user_address_state_or_territory,
            postal_code as user_address_postal_code,
            concat_ws(
                chr(10),
                nullif(street_address_1, ''),
                nullif(street_address_2, ''),
                nullif(street_address_3, ''),
                nullif(street_address_4, ''),
                nullif(street_address_5, '')
            ) as user_street_address
        from source
    )

select *
from cleaned
