with
    source as (

        select *
        from
            {{
                source(
                    "ol_warehouse_raw_data", "raw__mitxonline__app__postgres__flexiblepricing_currencyexchangerate"
                )
            }}

    )

    {{ deduplicate_raw_table(order_by="id", partition_columns="currency_code") }},
    renamed as (

        select
            id as currencyexchangerate_id,
            description as currencyexchangerate_description,
            currency_code as currencyexchangerate_currency_code,
            exchange_rate as currencyexchangerate_exchange_rate,
            {{ cast_timestamp_to_iso8601("created_on") }} as currencyexchangerate_created_on,
            {{ cast_timestamp_to_iso8601("updated_on") }} as currencyexchangerate_updated_on

        from most_recent_source

    )

select *
from renamed
