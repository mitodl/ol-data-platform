{#
    Which concepts each Learn course teaches, reviews, uses, assesses or requires,
    one row per (course, concept, relation), from the published knowledge release.
    concept_id is dim_concept's UUID and is already the sense the course uses, so
    Learn joins its copy to the concept graph by id and never resolves labels again.
    integrations__learn__concepts has each concept's name and descriptions under the
    same release_id.

    The rows were built by int__courseware__release_resource_concepts (the course's
    most recently extracted source-eligible run, learner-facing accepted concepts,
    relations an accepted claim supports) and copied into the release by
    ml.assets.courseware_release. A table, as integrations__learn__concepts is, and
    from the same published release.
#}
with published as (
    select release_id from {{ ref('int__courseware__published_release') }}
)

, resource_concepts as (
    select * from {{ source('courseware_intermediate', 'courseware_release_resource_concept') }}
)

select
    resource_concepts.release_id
    , resource_concepts.etl_source
    , resource_concepts.readable_id
    , resource_concepts.extracted_courserun_readable_id
    , resource_concepts.concept_id
    , resource_concepts.concept_name
    , resource_concepts.relation
    , resource_concepts.requirement_source
    , resource_concepts.claim_count
    , resource_concepts.first_scope_position
from resource_concepts
inner join published
    on resource_concepts.release_id = published.release_id
