-- One row per cluster_key
with category_proposal as (
    select * from {{ ref('int__feedback__category_proposal') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['cluster_key']) }} as feedback_cluster_pk
    , cluster_key
    , {{ dbt_utils.generate_surrogate_key(['category_slug']) }} as category_fk
    , category_label as proposed_category_label
    , category_description
    , cluster_status
    , cluster_run_id as proposed_in_cluster_run_id
    , {{ cast_timestamp_to_iso8601('proposed_at') }} as proposed_at
from category_proposal
