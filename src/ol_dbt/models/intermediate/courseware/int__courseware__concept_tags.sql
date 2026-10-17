{#
    Every place a known concept's label appears in courseware text, from the newest
    dictionary tagging run (ml.lib.courseware.tagging over int__courseware__extraction_sources
    and dim_concept_alias), with the matched phrase's mode and ambiguity flags. These
    are mentions only. Whether a source teaches the concept is the extractor's call.
#}
with runs as (
    select * from {{ source('courseware_intermediate', 'courseware_concept_tagging_run') }}
)

, latest_run as (
    select tagging_run_id
    from (
        select
            tagging_run_id
            , row_number() over (order by tagged_at desc) as run_rank
        from runs
    ) as ranked
    where run_rank = 1
)

, mentions as (
    select mention.*
    from {{ source('courseware_intermediate', 'courseware_concept_mention') }} as mention
    inner join latest_run
        on mention.tagging_run_id = latest_run.tagging_run_id
)

, aliases as (
    select alias.*
    from {{ source('courseware_intermediate', 'courseware_concept_tagging_alias') }} as alias
    inner join latest_run
        on alias.tagging_run_id = latest_run.tagging_run_id
)

select
    mentions.tagging_run_id
    , mentions.platform
    , mentions.courserun_readable_id
    , mentions.scope_id
    , mentions.scope_kind
    , mentions.source_ref
    , mentions.source_kind
    , mentions.concept_id
    , mentions.pattern_key
    , aliases.concept_label_key
    , aliases.variant
    , aliases.match_mode
    , aliases.is_short
    , aliases.is_qualified
    , aliases.is_single_word
    , aliases.has_multiple_concepts
    , aliases.is_ambiguous
    , mentions.matched_text
    , mentions.char_start
    , mentions.char_end
from mentions
inner join aliases
    on
        mentions.pattern_key = aliases.pattern_key
        and mentions.concept_id = aliases.concept_id
