with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__b2b_userb2bcontract') }}
)

{{ deduplicate_raw_table(raw_table='raw__mitxonline__app__postgres__b2b_userb2bcontract', partition_columns='id') }}

, cleaned as (
    select
        id as userb2bcontract_id,
        user_id,
        contract_page_id as contract_id,
        consented_to_data_sharing as userb2bcontract_consented_to_data_sharing,
        {{ cast_timestamp_to_iso8601("consent_modified_at") }} as userb2bcontract_consent_modified_at,
        {{ cast_timestamp_to_iso8601("created_on") }} as userb2bcontract_created_on,
        {{ cast_timestamp_to_iso8601("updated_on") }} as userb2bcontract_updated_on
    from most_recent_source
)

select * from cleaned
