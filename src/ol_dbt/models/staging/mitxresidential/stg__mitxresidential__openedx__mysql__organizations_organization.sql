-- MITx Residential open edX organizations (edx-organizations). An organization is not always the
-- org segment of a course key; organizations_organizationcourse is the authoritative link.

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitx__openedx__mysql__organizations_organization') }}
)

, cleaned as (

    select
        id as organization_id
        , name as organization_name
        , short_name as organization_short_name
        , description as organization_description
        , logo as organization_logo
        , active as organization_is_active
        , {{ cast_timestamp_to_iso8601('created') }} as organization_created_on
        , {{ cast_timestamp_to_iso8601('modified') }} as organization_updated_on
    from source
)

select * from cleaned
