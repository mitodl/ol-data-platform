with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__b2b_organizationpage') }}
)

, cleaned as (
    select
        page_ptr_id as organization_id,
        name as organization_name,
        org_key as organization_key,
        logo as organization_logo,
        description as organization_description,
        sso_organization_id
    from source
)

select * from cleaned
