with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__b2b_contractpage') }}
)

, cleaned as (
    select
        name as contract_name,
        active as contract_is_active,
        description as contract_description,
        organization_id as organization_id,
        page_ptr_id as wagtail_page_id,
        contract_start as contract_start_date,
        contract_end as contract_end_date,
        max_learners as contract_max_learners,
        integration_type as contract_integration_type,
        enrollment_fixed_price as contract_enrollment_fixed_price,
        membership_type as contract_membership_type
    from source
)

select * from cleaned
