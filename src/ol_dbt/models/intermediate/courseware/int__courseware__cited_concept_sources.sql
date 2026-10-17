-- Every source a current, verified claim quotes, per concept the claim names: where
-- the extractor itself located each concept, at the grain dictionary tagging tags.
with mentions as (
    select * from {{ ref('int__courseware__resolved_concept_mentions') }}
)

, evidence as (
    select * from {{ source('courseware_intermediate', 'courseware_claim_evidence') }}
    where verification_status in ('exact', 'fuzzy')
)

select
    mentions.platform
    , mentions.courserun_readable_id
    , mentions.scope_id
    , evidence.source_ref
    , mentions.concept_id
    , count(distinct concat(mentions.extraction_key, '|', cast(mentions.claim_ordinal as varchar))) as claim_count
from mentions
inner join evidence
    on
        mentions.extraction_key = evidence.extraction_key
        and mentions.claim_ordinal = evidence.claim_ordinal
group by
    mentions.platform
    , mentions.courserun_readable_id
    , mentions.scope_id
    , evidence.source_ref
    , mentions.concept_id
