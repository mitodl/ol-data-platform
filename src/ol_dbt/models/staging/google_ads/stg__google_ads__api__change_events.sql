with source as (
    select
        change_events.*
        , coalesce(customer_clients.customer_client__time_zone, {{ google_ads_default_time_zone() }})
        as account_time_zone
    from {{ source('ol_warehouse_raw_data', 'raw__google_ads__api__change_events') }} as change_events
    left join {{ source('ol_warehouse_raw_data', 'raw__google_ads__api__customer_clients') }} as customer_clients
        on change_events.customer_id = cast(customer_clients.customer_client__id as varchar)
)

select
    customer_id as account_id
    , change_event__resource_name as change_event_resource_name
    , {{ google_ads_local_to_utc('change_event__change_date_time', 'account_time_zone') }}
    as change_event_at
    , change_event__change_resource_type as change_resource_type
    , change_event__change_resource_name as change_resource_name
    , change_event__resource_change_operation as change_operation
    , change_event__changed_fields as change_changed_fields
    , change_event__client_type as change_client_type
    , change_event__campaign as campaign_resource_name
    , change_event__ad_group as ad_group_resource_name
    , change_event__old_resource as change_old_resource
    , change_event__new_resource as change_new_resource
    , retrieved_at as change_event_retrieved_at
    , _dlt_load_id as change_event_dlt_load_id
from source
