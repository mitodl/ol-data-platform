{#
    What the next knowledge release would be built from: one row binding the
    resolution run dim_concept was built from, the newest graph run and the
    resolution run that graph was computed over, the approved description prompt,
    the current extractions of source-eligible runs and the source policy. The
    courseware_knowledge_release asset writes this row onto the release with the
    projections int__courseware__release_concepts and
    int__courseware__release_resource_knowledge, and refuses to publish a candidate
    whose graph was not computed over its resolution run (is_graph_current), as when
    a resolution run merged concepts and the graph build after it failed.

    sense_assignment_version stays null until claim-level sense assignment exists.
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

, descriptions as (
    select max(approved_prompt_version) as description_prompt_version
    from {{ ref('int__courseware__concept_description_inputs') }}
)

, concepts as (
    select count(case when is_awaiting_description then 1 end) as undescribed_concept_count
    from {{ ref('int__courseware__release_concepts') }}
)

select
    registry.resolution_run_id
    , resolution.resolver_id
    , resolution.resolution_prompt_version
    , cast(null as varchar) as sense_assignment_version
    , graph.graph_run_id
    , graph.graph_version
    , graph.graph_resolution_run_id
    , coalesce(graph.graph_resolution_run_id = registry.resolution_run_id, false) as is_graph_current
    , descriptions.description_prompt_version
    , concepts.undescribed_concept_count
    , extractions.extraction_selection_checksum
    , extractions.extraction_count
    , eligibility.source_policy_version
from registry
cross join eligibility
cross join extractions
cross join descriptions
cross join concepts
left join resolution
    on registry.resolution_run_id = resolution.resolution_run_id
left join graph
    on true
