{#
    The concepts Learn can show to learners, from the published knowledge release:
    one row per learner-facing concept with its descriptions and how many courses
    teach it, for concept chips, tooltips and a concept page. Pairs with
    integrations__learn__resource_concepts, which lists each course's concepts by id
    from the same release; release_id says which.

    The rows were built by int__courseware__release_concepts (accepted concepts,
    source-eligible runs only) and copied into the release by
    ml.assets.courseware_release. A table like the other Learn views, because
    delivery reads them from Iceberg; the publication pointer rebuilds this,
    integrations__learn__resource_knowledge and integrations__learn__resource_concepts
    in the same dbt run, and a reader of more than one checks their release_id
    matches.
#}
with published as (
    select release_id from {{ ref('int__courseware__published_release') }}
)

, concepts as (
    select * from {{ source('courseware_intermediate', 'courseware_release_concept') }}
)

select
    concepts.release_id
    , concepts.concept_id
    , concepts.name
    , concepts.concept_kind
    , concepts.short_description
    , concepts.description
    , concepts.review_status
    , concepts.description_model_id
    , concepts.teaching_course_count
    , concepts.course_count
from concepts
inner join published
    on concepts.release_id = published.release_id
where concepts.is_learner_facing
