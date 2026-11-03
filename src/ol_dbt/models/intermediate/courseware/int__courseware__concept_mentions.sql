{#
    Every place a current, verified claim names a concept, one row per (claim, concept
    label): the concept, assumed-knowledge and declared-prerequisite claims by their
    own label, and the assessments and learning outcomes by each concept label they
    list. This is what dim_concept and the concept bridges are built from.
#}
with claims as (
    select * from {{ ref('int__courseware__claims') }}
    where is_current and is_verified
)

, own_labels as (
    select
        platform
        , courserun_readable_id
        , scope_id
        , scope_kind
        , scope_position
        , extraction_key
        , claim_ordinal
        , extracted_at
        , claim_kind
        , teaching_status
        , concept_kind
        , description
        , label
        , concept_label_key
        , prerequisite_kind
        , review_status
    from claims
    where concept_label_key is not null
)

, referenced_labels as (
    select
        claims.platform
        , claims.courserun_readable_id
        , claims.scope_id
        , claims.scope_kind
        , claims.scope_position
        , claims.extraction_key
        , claims.claim_ordinal
        , claims.extracted_at
        , claims.claim_kind
        , cast(null as varchar) as teaching_status
        , cast(null as varchar) as concept_kind
        , cast(null as varchar) as description
        , {{ json_extract_scalar('referenced.concept_label', "'$'") }} as label
        , cast(null as varchar) as prerequisite_kind
        , claims.review_status
    from claims
    cross join {{ unnest_json_array('claims.related_concept_labels_json', 'referenced', 'concept_label') }} -- noqa
    where claims.claim_kind in ('assessment_expectation', 'learning_outcome')
)

select
    platform
    , courserun_readable_id
    , scope_id
    , scope_kind
    , scope_position
    , extraction_key
    , claim_ordinal
    , extracted_at
    , claim_kind
    , teaching_status
    , concept_kind
    , description
    , label
    , concept_label_key
    , prerequisite_kind
    , review_status
from own_labels

union all

select
    platform
    , courserun_readable_id
    , scope_id
    , scope_kind
    , scope_position
    , extraction_key
    , claim_ordinal
    , extracted_at
    , claim_kind
    , teaching_status
    , concept_kind
    , description
    , label
    , {{ courseware_label_key('label') }} as concept_label_key
    , prerequisite_kind
    , review_status
from referenced_labels
-- a label of only symbols has no key, like the own labels above
where {{ courseware_label_key('label') }} is not null
