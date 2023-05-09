--- source table contains duplicated rows per primary key. This means there could be multiple copies of the same record,
--- thus deduplicate records here using _airbyte_emitted_at

with source as (

    select *
    from {{ source('ol_warehouse_raw_data', 'raw__thirdparty__salesforce__Opportunity') }}
    where isdeleted = false

)

, source_sorted as (
    select
        *
        , row_number() over (partition by id order by _airbyte_emitted_at desc) as row_num
    from source
)

, most_recent_source as (
    select *
    from source_sorted
    where row_num = 1
)

, renamed as (

    select
        id as opportunity_id
        , name as opportunity_name
        , type as opportunity_type
        , business_type__c as opportunity_business_type
        , agreement_type__c as opportunity_agreement_type
        , mit_is_sales_lead__c as opportunity_mit_is_sales_lead
        , revenue_share_or_commission_contract__c as opportunity_revenue_share_or_commission_contract
        , revenue_share_or_commission_amount__c as opportunity_revenue_share_or_commission_amount
        , stagename as opportunity_stage
        , amount as opportunity_amount
        , probability as opportunity_probability
        , iswon as opportunity_is_won
        , isclosed as opportunity_is_closed
        , enrollment_codes_redeemed__c as opportunity_num_enrollment_codes_redeemed
        , of_seats_purchased__c as opportunity_num_of_seats_purchased
        , price_per_seat__c as opportunity_price_per_seat
        , leadsource as opportunity_leadsource
        , nextstep as opportunity_nextstep
        , hasopportunitylineitem as opportunity_has_lineitem
        , data_quality_score__c as opportunity_data_quality_score
        , data_quality_description__c as opportunity_data_quality_description
        ,{{ cast_date_to_iso8601('closedate') }} as opportunity_close_date
        ,{{ cast_timestamp_to_iso8601('createddate') }} as opportunity_created_on
        ,{{ cast_timestamp_to_iso8601('systemmodstamp') }} as opportunity_modified_on
    from most_recent_source
)

select * from renamed
