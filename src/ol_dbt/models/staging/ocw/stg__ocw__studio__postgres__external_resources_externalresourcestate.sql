with
    source as ({{ live_raw_rows("raw__ocw__studio__postgres__external_resources_externalresourcestate") }})

select
    id as externalresourcestate_id
    , content_id as websitecontent_id
    , status as externalresourcestate_status
    , external_url_response_code as externalresourcestate_url_response_code
    , wayback_url as externalresourcestate_wayback_url
    , {{ cast_timestamp_to_iso8601('last_checked') }} as externalresourcestate_last_checked_on
    , {{ cast_timestamp_to_iso8601('updated_on') }} as externalresourcestate_updated_on
from source
