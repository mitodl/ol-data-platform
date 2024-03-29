with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_order') }}

)

, renamed as (
    select
        id as order_id
        , status as order_state
        , cast(total_price_paid as decimal(38, 2)) as order_total_price_paid
        , purchaser_id as order_purchaser_user_id
        , tax_country_code as order_tax_country_code
        , cast(tax_rate as decimal(38, 2)) as order_tax_rate
        , tax_rate_name as order_tax_rate_name
        , cast((total_price_paid * (tax_rate / 100)) as decimal(38, 2)) as order_tax_amount
        , cast(
            (total_price_paid * (tax_rate / 100)) + total_price_paid
            as decimal(38, 2)
        ) as order_total_price_paid_plus_tax
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as order_updated_on
        ,{{ cast_timestamp_to_iso8601('created_on') }} as order_created_on
    from source
)

select * from renamed
