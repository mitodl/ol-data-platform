{#
    Every extracted claim with its extraction's scope and version, its evidence counts,
    and the key that groups its label with the same label elsewhere.

    is_current marks the newest extraction of each scope part, so rollups read one
    extractor version per scope while older versions stay comparable. A claim is
    verified when at least one of its passages was found in the source text.

    concept_label_key is a string normalization only (case, punctuation, spacing).
    Resolving synonyms and variants across courses ("k-means" and "k-means
    clustering") is a separate step that will map these keys onto dim_concept.
#}
with extractions as (
    select * from {{ source('courseware_intermediate', 'courseware_claim_extraction') }}
)

, claims as (
    select * from {{ source('courseware_intermediate', 'courseware_claim') }}
)

, evidence as (
    select * from {{ source('courseware_intermediate', 'courseware_claim_evidence') }}
)

, ranked_extractions as (
    select
        *
        , row_number() over (
            partition by platform, courserun_readable_id, scope_id, scope_part
            order by extracted_at desc
        ) = 1 as is_current
    from extractions
)

, evidence_counts as (
    select
        extraction_key
        , claim_ordinal
        , count(*) as evidence_count
        , count(case when verification_status in ('exact', 'fuzzy') then 1 end) as verified_evidence_count
    from evidence
    group by extraction_key, claim_ordinal
)

select
    ranked_extractions.platform
    , ranked_extractions.courserun_readable_id
    , ranked_extractions.scope_id
    , ranked_extractions.scope_kind
    , ranked_extractions.scope_title
    , ranked_extractions.scope_position
    , ranked_extractions.scope_part
    , ranked_extractions.extraction_key
    , ranked_extractions.extractor_id
    , ranked_extractions.model_id
    , ranked_extractions.prompt_version
    , ranked_extractions.extracted_at
    , ranked_extractions.is_current
    , claims.claim_ordinal
    , claims.claim_kind
    , claims.label
    , claims.description
    , claims.concept_kind
    , claims.teaching_status
    , claims.how_used
    , claims.flagged_as_prior
    , claims.bloom_process
    , claims.knowledge_type
    , claims.prerequisite_kind
    , claims.course_ref
    , claims.related_concept_labels_json
    , case
        when claims.claim_kind in ('concept', 'assumed_knowledge', 'declared_prerequisite')
            then {{ strip_whitespace(regexp_replace_all('lower(claims.label)', "'[^a-z0-9]+'", "' '")) }}
    end as concept_label_key
    , coalesce(evidence_counts.evidence_count, 0) as evidence_count
    , coalesce(evidence_counts.verified_evidence_count, 0) as verified_evidence_count
    , coalesce(evidence_counts.verified_evidence_count, 0) > 0 as is_verified
from claims
inner join ranked_extractions
    on claims.extraction_key = ranked_extractions.extraction_key
left join evidence_counts
    on
        claims.extraction_key = evidence_counts.extraction_key
        and claims.claim_ordinal = evidence_counts.claim_ordinal
