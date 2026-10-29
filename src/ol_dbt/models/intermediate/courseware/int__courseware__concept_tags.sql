{#
    Every place a known concept's label appears in courseware text, from the newest
    dictionary tagging run (ml.lib.courseware.tagging over int__courseware__extraction_sources
    and dim_concept_alias), with the matched phrase's mode and ambiguity flags. These
    are mention candidates only. Whether a source teaches the concept is the
    extractor's call.

    A run tags with the concept ids current when it ran. concept_id follows the
    newest resolution run's redirects (a concept merged since then becomes its
    survivor), so it joins to the extractor's resolved mentions; tagged_concept_id
    is the id as tagged, which the run's dictionary rows are keyed by. A phrase the
    run gave to two concepts that have since merged tagged each of its spans twice,
    so one row per span and current concept is kept: the one with the lowest
    tagged_concept_id.

    A label can name several concepts (its senses), and the dictionary emits a
    candidate for each. Which one a mention means is the course run's sense of the
    label (int__courseware__concept_sense_assignments), never the dictionary's:
    - course_sense: the run's sense of the label is this concept;
    - needs_review: the run's sense of the label is in the review queue;
    - candidate: the extractor never named the label in this run, so nothing has
      decided its sense here, even when the label has only one known sense.
    A candidate for a concept other than the run's sense is dropped, so one sense is
    not spread to every course that uses the label. Ambiguity flags are review
    signals and decide nothing.
#}
with mentions as (
    select mention.*
    from {{ source('courseware_intermediate', 'courseware_concept_mention') }} as mention
    inner join (select distinct tagging_run_id from {{ ref('int__courseware__concept_tagging_aliases') }}) as latest_run
        on mention.tagging_run_id = latest_run.tagging_run_id
)

, aliases as (
    select * from {{ ref('int__courseware__concept_tagging_aliases') }}
)

, sense_assignments as (
    select * from {{ ref('int__courseware__concept_sense_assignments') }}
)

, redirected as (
    select
        mentions.tagging_run_id
        , mentions.platform
        , mentions.courserun_readable_id
        , mentions.scope_id
        , mentions.scope_kind
        , mentions.source_ref
        , mentions.source_kind
        , aliases.concept_id
        , aliases.tagged_concept_id
        , mentions.pattern_key
        , aliases.concept_label_key
        , aliases.variant
        , aliases.match_mode
        , aliases.is_short
        , aliases.is_qualified
        , aliases.is_single_word
        , aliases.has_multiple_concepts
        , aliases.is_ambiguous
        , case
            when sense_assignments.sense_status = 'needs_review' then 'needs_review'
            when sense_assignments.concept_id = aliases.concept_id then 'course_sense'
            when sense_assignments.concept_id is null then 'candidate'
        end as sense_status
        , mentions.matched_text
        , mentions.char_start
        , mentions.char_end
    from mentions
    inner join aliases
        on
            mentions.pattern_key = aliases.pattern_key
            and mentions.concept_id = aliases.tagged_concept_id
    left join sense_assignments
        on
            aliases.concept_label_key = sense_assignments.concept_label_key
            and mentions.platform = sense_assignments.platform
            and mentions.courserun_readable_id = sense_assignments.courserun_readable_id
)

select
    tagging_run_id
    , platform
    , courserun_readable_id
    , scope_id
    , scope_kind
    , source_ref
    , source_kind
    , concept_id
    , tagged_concept_id
    , pattern_key
    , concept_label_key
    , variant
    , match_mode
    , is_short
    , is_qualified
    , is_single_word
    , has_multiple_concepts
    , is_ambiguous
    , sense_status
    , matched_text
    , char_start
    , char_end
from (
    select
        *
        , row_number() over (
            partition by platform, courserun_readable_id, source_ref, char_start, concept_id
            order by tagged_concept_id
        ) as span_rank
    from redirected
    -- the run uses the label in another sense
    where sense_status is not null
) as ranked
where span_rank = 1
