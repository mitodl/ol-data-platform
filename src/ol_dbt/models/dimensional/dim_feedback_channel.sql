with feedback_channels as (
    select * from {{ ref('feedback_channels') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['channel_slug']) }} as feedback_channel_pk
    , channel_slug
    , channel_name
    , is_solicited
from feedback_channels
