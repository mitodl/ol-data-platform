with feedback_sources as (
    select * from {{ ref('feedback_sources') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['source_slug']) }} as feedback_source_pk
    , source_slug
    , source_name
    , source_medium
    , source_audience_scope
    , is_course_scoped
    , is_conversational
from feedback_sources
