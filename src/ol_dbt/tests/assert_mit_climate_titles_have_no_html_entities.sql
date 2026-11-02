{{ config(severity="warn") }}

-- integrations__learn__mit_climate_articles decodes only the entities html_unescape knows,
-- where MIT Learn's legacy ETL decoded all of them with Python's html.unescape. A row here
-- has an entity that reaches MIT Learn undecoded; add it to html_unescape.
select
    readable_id
    , title
from {{ ref('integrations__learn__mit_climate_articles') }}
where {{ regexp_like('title', "'&#?[a-zA-Z0-9]+;'") }}
