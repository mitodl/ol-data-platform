with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__b2b_organizationpage') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at', partition_columns='page_ptr_id') }}

, cleaned as (
    select
        page_ptr_id as organization_id,
        name as organization_name,
        org_key as organization_key,
        logo as organization_logo,
        description as organization_description,
        sso_organization_id
    from most_recent_source
)

select * from cleaned
