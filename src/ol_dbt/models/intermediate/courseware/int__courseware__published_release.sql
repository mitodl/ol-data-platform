{#
    The published knowledge release: the release courseware_release_publication
    pointed to most recently, with its manifest. Every learner-facing projection
    selects its rows through this one row, so a promotion or a rollback moves
    them all to the same release, and a candidate that was never published (its graph build
    failed, say) is never read.
#}
with publications as (
    select
        release_id
        , published_at
        , row_number() over (order by published_at desc) as publication_rank
    from {{ source('courseware_intermediate', 'courseware_release_publication') }}
)

select
    releases.release_id
    , publications.published_at
    , releases.built_at
    , releases.output_schema_version
    , releases.resolution_run_id
    , releases.sense_assignment_version
    , releases.graph_run_id
    , releases.graph_version
    , releases.description_prompt_version
    , releases.extraction_selection_checksum
    , releases.source_policy_version
from publications
inner join {{ source('courseware_intermediate', 'courseware_release') }} as releases
    on publications.release_id = releases.release_id
where publications.publication_rank = 1
