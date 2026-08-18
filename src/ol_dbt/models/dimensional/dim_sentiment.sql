with sentiments as (
    select * from {{ ref('sentiments') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['sentiment_slug']) }} as sentiment_pk
    , sentiment_slug
    -- the rollup grouping for trend charts. Degenerate with sentiment_slug while the
    -- vocabulary is only positive/neutral/negative, so it stays null until the
    -- sentiment task introduces a finer label set for it to collapse.
    , cast(polarity_score_bucket as varchar) as polarity_score_bucket
from sentiments
