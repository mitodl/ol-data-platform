with sentiments as (
    select * from {{ ref('sentiments') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['sentiment_slug']) }} as sentiment_pk
    , sentiment_slug
    -- degenerate with sentiment_slug while the vocabulary is only three values, so it
    -- stays null until a finer label set gives it something to collapse
    , cast(polarity_score_bucket as varchar) as polarity_score_bucket
from sentiments
