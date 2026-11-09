-- Neighbourhoods of related concepts from the newest graph run (Leiden over every
-- relation family). community_pk is minted when a community first appears and
-- carried forward by member overlap, so a community can be followed as courses are
-- added; first_seen_run_id says when it appeared.
with communities as (
    select community.*
    from {{ source('courseware_intermediate', 'courseware_concept_graph_community') }} as community
    inner join {{ ref('int__courseware__latest_concept_graph_run') }} as latest
        on community.graph_run_id = latest.graph_run_id
)

select
    community_id as community_pk
    , concept_count
    , top_concepts_json
    , first_seen_run_id
    , graph_run_id
from communities
