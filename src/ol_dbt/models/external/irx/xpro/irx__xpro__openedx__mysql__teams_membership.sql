with teams_courseteam as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__mysql__teams_courseteam') }}
)

, teams_courseteammembership as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__mysql__teams_courseteammembership') }}
)

select
    teams_courseteammembership.id
    , teams_courseteammembership.user_id
    , teams_courseteammembership.team_id
from teams_courseteam
inner join teams_courseteammembership on teams_courseteam.id = teams_courseteammembership.team_id
