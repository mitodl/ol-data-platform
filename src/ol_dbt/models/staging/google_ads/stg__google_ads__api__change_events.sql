with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__google_ads__api__change_events') }}
)

select
    customer_id
    , change_event__resource_name as change_event_resource_name
    , change_event__change_date_time as change_event_at
    , change_event__change_resource_type as change_resource_type
    , change_event__change_resource_name as change_resource_name
    , change_event__resource_change_operation as change_operation
    , change_event__changed_fields as change_changed_fields
    , change_event__client_type as change_client_type
    , change_event__campaign as campaign_resource_name
    , change_event__ad_group as ad_group_resource_name
    , change_event__old_resource as change_old_resource
    , change_event__new_resource as change_new_resource
    , retrieved_at
from source
