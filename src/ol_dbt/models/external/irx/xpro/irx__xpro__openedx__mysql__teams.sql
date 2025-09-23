select team_size, description, team_id
from {{ source("ol_warehouse_raw_data", "raw__xpro__openedx__mysql__teams_courseteam") }}
