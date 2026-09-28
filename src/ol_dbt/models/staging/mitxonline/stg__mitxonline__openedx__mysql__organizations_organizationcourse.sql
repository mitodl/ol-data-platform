-- MITx Online open edX course run to organization links (edx-organizations)

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__openedx__mysql__organizations_organizationcourse') }}
)

, cleaned as (

    select
        id as organizationcourse_id
        , course_id as courserun_readable_id
        , organization_id
        , active as organizationcourse_is_active
        , {{ cast_timestamp_to_iso8601('created') }} as organizationcourse_created_on
        , {{ cast_timestamp_to_iso8601('modified') }} as organizationcourse_updated_on
    from source
)

select * from cleaned
