{#
    What each Learn course teaches, assesses and expects, from the published
    knowledge release, keyed as Learn keys the course (etl_source, readable_id).
    release_id says which release. The course's concepts are published by id, with
    the sense each course uses, in integrations__learn__resource_concepts from the
    same release, not as label arrays here.

    The rows were built by int__courseware__release_resource_knowledge (the course's
    most recently extracted source-eligible run, accepted concepts and claims only)
    and copied into the release by ml.assets.courseware_release. context_document is
    the deterministic markdown the embedding substrate's
    resource_knowledge_markdown_v1 builder embeds, rendered from the release's
    concept labels and statements. A table, as integrations__learn__concepts is, and
    from the same published release.
#}
with published as (
    select release_id from {{ ref('int__courseware__published_release') }}
)

, knowledge as (
    select * from {{ source('courseware_intermediate', 'courseware_release_resource_knowledge') }}
)

select
    knowledge.release_id
    , knowledge.etl_source
    , knowledge.readable_id
    , knowledge.extracted_courserun_readable_id
    , knowledge.course_title
    , knowledge.skills
    , knowledge.learning_outcomes
    , knowledge.audience
    , knowledge.context_document
from knowledge
inner join published
    on knowledge.release_id = published.release_id
