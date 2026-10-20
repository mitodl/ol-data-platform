-- One row per quoted passage. char_start/char_end index the source_text of the
-- matching int__courseware__extraction_sources row, so a passage can be shown in
-- context from the warehouse alone.
with evidence as (
    select * from {{ source('courseware_intermediate', 'courseware_claim_evidence') }}
)

, extractions as (
    select * from {{ ref('dim_courseware_extraction') }}
)

, content as (
    select * from {{ ref('dim_course_content') }}
    where is_latest
)

select
    {{ dbt_utils.generate_surrogate_key(['evidence.extraction_key', 'evidence.claim_ordinal', 'evidence.evidence_ordinal']) }}
    as courseware_claim_evidence_pk
    , {{ dbt_utils.generate_surrogate_key(['evidence.extraction_key', 'evidence.claim_ordinal']) }} as courseware_claim_fk
    , extractions.platform
    , extractions.courserun_readable_id
    -- the cited block, when the source is a block rather than an exported file
    , content.content_block_pk as content_block_fk
    , evidence.evidence_ordinal
    , evidence.source_ref
    , evidence.passage
    , evidence.verification_status
    , evidence.char_start
    , evidence.char_end
from evidence
inner join extractions
    on evidence.extraction_key = extractions.extraction_key
left join content
    on
        extractions.platform = content.platform
        and extractions.courserun_readable_id = content.courserun_readable_id
        and evidence.source_ref = content.block_id
