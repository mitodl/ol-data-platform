{{ config(severity="warn") }}

-- int__mitpe__learning_resources decodes only the HTML entities the MIT PE feed has been
-- seen to use (see the html_unescape macro), where MIT Learn's legacy ETL decoded all of
-- them with Python's html.unescape. A row here has an entity that reaches MIT Learn
-- undecoded; add it to html_unescape.
select
    readable_id
    , title
    , {{ array_join('topics', ' | ') }} as topics
    , {{ array_join('instructors', ' | ') }} as instructors
from {{ ref('int__mitpe__learning_resources') }}
where
    {{ regexp_like('title', "'&#?[a-zA-Z0-9]+;'") }}
    or {{ regexp_like(array_join('topics', ' | '), "'&#?[a-zA-Z0-9]+;'") }}
    or {{ regexp_like(array_join('instructors', ' | '), "'&#?[a-zA-Z0-9]+;'") }}
