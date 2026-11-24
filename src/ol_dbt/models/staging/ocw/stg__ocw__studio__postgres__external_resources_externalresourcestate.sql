with
    source as ({{ live_raw_rows("raw__ocw__studio__postgres__external_resources_externalresourcestate") }})

-- content_id is unique at the source, but a state row deleted in the Django admin is
-- recreated under a new id and the xmin sync keeps the old one, so keep the newest.
, latest as (
    select
        *
        , row_number() over (partition by content_id order by updated_on desc, id desc) as row_num
    from source
)

select
    id as externalresourcestate_id
    , content_id as websitecontent_id
    , status as externalresourcestate_status
    , external_url_response_code as externalresourcestate_url_response_code
    , wayback_url as externalresourcestate_wayback_url
    , {{ cast_timestamp_to_iso8601('last_checked') }} as externalresourcestate_last_checked_on
    , {{ cast_timestamp_to_iso8601('updated_on') }} as externalresourcestate_updated_on
from latest
where row_num = 1
