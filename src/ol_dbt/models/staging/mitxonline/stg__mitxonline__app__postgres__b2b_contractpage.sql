with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__b2b_contractpage') }}
)

, cleaned as (
    select
        page_ptr_id as b2b_contract_id,
        name as b2b_contract_name,
        active as b2b_contract_is_active,
        description as b2b_contract_description,
        organization_id,
        contract_start as b2b_contract_start_date,
        contract_end as b2b_contract_end_date,
        max_learners as b2b_contract_max_learners,
        integration_type as b2b_contract_integration_type,
        enrollment_fixed_price as b2b_contract_enrollment_fixed_price,
        membership_type as b2b_contract_membership_type
    from source
)

select * from cleaned
