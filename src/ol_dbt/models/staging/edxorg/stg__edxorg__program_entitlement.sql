with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__edxorg__program_entitlement') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at' , partition_columns='"program title", email') }}

, cleaned as (
    select
        "program type" as program_type
        , "program title" as program_title
        , "partner key" as organization_key
        , email as user_email
        , cast("user id" as integer) as user_id
        , cast(entitlements as integer) as number_of_entitlements
        , cast("redeemed entitlements" as integer) as number_of_redeemed_entitlements
        , cast(date_parse("purchase date", '%m/%d/%Y') as date) as purchase_date
        , cast(date_parse("expiration date", '%m/%d/%Y') as date) as expiration_date
    from most_recent_source
)
select * from cleaned
