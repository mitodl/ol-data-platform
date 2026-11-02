with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__edxorg__program_entitlement') }}
)

{{ deduplicate_raw_table(
    raw_table='raw__edxorg__program_entitlement',
    partition_columns=adapter.quote('program title') ~ ', email',
    then_by="cast(" ~ date_parse(adapter.quote('purchase date'), "'%m/%d/%Y'") ~ " as date)"
) }}

, cleaned as (
    select
        {{ adapter.quote('program type') }} as program_type
        , {{ adapter.quote('program title') }} as program_title
        , {{ adapter.quote('partner key') }} as organization_key
        , email as user_email
        , username as user_username
        , cast({{ adapter.quote('user id') }} as integer) as user_id
        , nullif(
              trim(
                 concat_ws(' ',
                    nullif(trim({{ adapter.quote('first name') }}), ''),
                    nullif(trim({{ adapter.quote('last name') }}), '')
                  )
              ),
              ''
        ) as user_full_name
        , cast(entitlements as integer) as number_of_entitlements
        , cast({{ adapter.quote('redeemed entitlements') }} as integer) as number_of_redeemed_entitlements
        , cast({{ date_parse(adapter.quote('purchase date'), "'%m/%d/%Y'") }} as date) as purchase_date
        , cast({{ date_parse(adapter.quote('expiration date'), "'%m/%d/%Y'") }} as date) as expiration_date
    from most_recent_source
)
select * from cleaned
