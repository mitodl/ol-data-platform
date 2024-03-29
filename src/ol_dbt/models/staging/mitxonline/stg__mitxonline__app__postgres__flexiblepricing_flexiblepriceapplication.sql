with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__flexiblepricing_flexibleprice') }}

)

, renamed as (

    select
        id as flexiblepriceapplication_id
        , status as flexiblepriceapplication_status
        , tier_id as flexiblepricetier_id
        , user_id
        , income_usd as flexiblepriceapplication_income_usd
        , justification as flexiblepriceapplication_justification
        , original_income as flexiblepriceapplication_original_income
        , cms_submission_id as flexiblepriceapplication_cms_submission_id
        , country_of_income as flexiblepriceapplication_country_of_income
        , original_currency as flexiblepriceapplication_original_currency
        , country_of_residence as flexiblepriceapplication_country_of_residence
        , courseware_object_id
        , courseware_content_type_id as contenttype_id
        ,{{ cast_timestamp_to_iso8601('date_exchange_rate') }} as flexiblepriceapplication_exchange_rate_timestamp
        ,{{ cast_timestamp_to_iso8601('date_documents_sent') }} as flexiblepriceapplication_date_documents_sent
        ,{{ cast_timestamp_to_iso8601('created_on') }} as flexiblepriceapplication_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as flexiblepriceapplication_updated_on

    from source

)

select * from renamed
