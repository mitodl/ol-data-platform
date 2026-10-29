{#
    What the next knowledge release would be built from: one row binding the
    resolution run dim_concept was built from, the newest graph run and the
    resolution run that graph was computed over, the approved description prompt,
    the current extractions of source-eligible runs and the source policy. The
    courseware_knowledge_release asset writes this row onto the release with the
    projections int__courseware__release_concepts,
    int__courseware__release_resource_knowledge and
    int__courseware__release_resource_concepts, and refuses to publish a candidate
    whose graph was not computed over its resolution run (is_graph_current), as when
    a resolution run merged concepts and the graph build after it failed.

    It reads every candidate projection, so dbt builds it only after all of them,
    and records each one's row count and dbt_invocation_id, the dbt invocation that
    built it. The release asset, which runs on this model alone, refuses projections
    whose rows another invocation built since, as when a projection is rebuilt
    alone after this model, so it never copies projections this manifest did not
    read.

    extraction_selection_checksum hashes the current extraction keys. A
    courseware_claims_v2 key hashes the text stage's key (its exact text input) and
    the context stage's input digest, so the checksum moves when either stage's
    input changes, a header edit included.

    The concept ids come from the sense assignment snapshot of the same resolution
    run (int__courseware__concept_sense_assignments), so resolution_run_id pins it;
    sense_assignment_version is that run's sense_prompt_version, the version of the
    sense step that wrote it. A run written before sense assignment has none, and the
    release asset refuses it.

    Deploy order: dim_concept and everything built from it, this model included,
    fail until the courseware_resolution asset has run with sense assignment.
    That run creates courseware_concept_alias and courseware_concept_sense_assignment
    and adds split_from_concept_id to the registry and sense_prompt_version to
    courseware_resolution_run, so this model reads sense_prompt_version directly.
#}
with registry as (
    select max(resolution_run_id) as resolution_run_id
    from {{ ref('int__courseware__concept_registry') }}
)

, resolution as (
    select
        runs.resolution_run_id
        , runs.resolver_id
        , runs.prompt_version as resolution_prompt_version
        , runs.sense_prompt_version as sense_assignment_version
    from {{ source('courseware_intermediate', 'courseware_resolution_run') }} as runs
    inner join registry
        on runs.resolution_run_id = registry.resolution_run_id
)

, graph as (
    select
        runs.graph_run_id
        , runs.graph_version
        , runs.resolution_run_id as graph_resolution_run_id
    from {{ source('courseware_intermediate', 'courseware_concept_graph_run') }} as runs
    inner join {{ ref('int__courseware__latest_concept_graph_run') }} as latest
        on runs.graph_run_id = latest.graph_run_id
)

, eligibility as (
    select max(source_policy_version) as source_policy_version
    from {{ ref('int__courseware__source_eligibility') }}
)

, extractions as (
    select
        count(*) as extraction_count
        , {{ md5_hex(array_join(
            'array_agg(extractions.extraction_key order by extractions.extraction_key)', ','
        )) }} as extraction_selection_checksum
    from {{ ref('dim_courseware_extraction') }} as extractions
    inner join {{ ref('int__courseware__source_eligibility') }} as eligibility
        on
            extractions.platform = eligibility.platform
            and extractions.courserun_readable_id = eligibility.courserun_readable_id
    where
        extractions.is_current
        and eligibility.is_source_eligible
)

-- from the seed, not the description inputs, which a policy excluding every
-- described run empties while the seed still approves a prompt
, descriptions as (
    select approval.prompt_version as description_prompt_version
    from ({{ courseware_approved_description_prompt() }}) as approval
)

, concepts as (
    select
        count(*) as concept_count
        , count(case when is_awaiting_description then 1 end) as undescribed_concept_count
        , max(dbt_invocation_id) as concepts_invocation_id
    from {{ ref('int__courseware__release_concepts') }}
)

, resources as (
    select
        count(*) as resource_count
        , max(dbt_invocation_id) as resources_invocation_id
    from {{ ref('int__courseware__release_resource_knowledge') }}
)

, resource_concepts as (
    select
        count(*) as resource_concept_count
        , max(dbt_invocation_id) as resource_concepts_invocation_id
    from {{ ref('int__courseware__release_resource_concepts') }}
)

select
    registry.resolution_run_id
    , resolution.resolver_id
    , resolution.resolution_prompt_version
    , resolution.sense_assignment_version
    , graph.graph_run_id
    , graph.graph_version
    , graph.graph_resolution_run_id
    , coalesce(graph.graph_resolution_run_id = registry.resolution_run_id, false) as is_graph_current
    , descriptions.description_prompt_version
    , concepts.undescribed_concept_count
    , concepts.concept_count
    , resources.resource_count
    , resource_concepts.resource_concept_count
    , concepts.concepts_invocation_id
    , resources.resources_invocation_id
    , resource_concepts.resource_concepts_invocation_id
    , extractions.extraction_selection_checksum
    , extractions.extraction_count
    , eligibility.source_policy_version
from registry
cross join eligibility
cross join extractions
cross join descriptions
cross join concepts
cross join resources
cross join resource_concepts
left join resolution
    on registry.resolution_run_id = resolution.resolution_run_id
left join graph
    on true
