{#
    Experimental. Which other course runs might help prepare for each concept a
    course run requires, one row per (requirement, providing run), with the evidence
    for the match in separate columns. A requirement is a requires row of
    bridge_courserun_concept, declared or inferred, passing its support rule or not
    (requirement_passes_support_rule). Requirements no other run covers have no rows.

    This is preparation evidence for ranking candidates, not a readiness verdict.
    Nothing here says the provider teaches the concept to the depth the requiring
    course needs: reviewing a concept can assume it is already known, a close
    concept need not be interchangeable preparation, and the narrower concepts a
    provider omits may be exactly the ones the target needs. A verdict needs a
    versioned preparation specification for the target course, which does not
    exist yet, so this model gives none.

    A providing run is a current run of a different course (see course_key below),
    and match_type is the closest of these:
      direct    it teaches or reviews the concept
      close     it teaches or reviews a close equivalent (bridge_concept_relation
                close: nearly interchangeable, not merged)
      narrower  it teaches or reviews some of the concept's narrower concepts
    provider_relation says whether a direct or close provider teaches the concept or
    only reviews it.

    narrower is for umbrella requirements such as "Python programming", which courses
    teach only as their parts (lists, functions, modules). The narrower concepts of a
    required concept are those reachable down passing broader edges in at most
    courseware_prerequisite_max_narrower_hops edges that have any passing row in
    bridge_courserun_concept, i.e. appear in some current course run. Each row lists
    the ones the provider teaches or reviews (covered_narrower_concept_fks) and the
    rest (missing_narrower_concept_fks), and match_depth is the fewest hops to a
    covered one (0 for direct and close). That set is what the corpus happens to
    contain, not what the target needs, so it is reported as evidence and never
    turned into a share or a threshold: adding an unrelated course can add narrower
    concepts to the missing list and the count, and nothing else in a provider's row. The walk does not
    start from close equivalents too: on the pilot that pulled the mathematical
    "function" tree (logistic, softmax) under "Python function".

    Uncertainty stays in its own columns rather than a score: requirement_source and
    requirement_passes_support_rule for the requirement (that rule counts other
    courses requiring the concept, so it can change when a course is added; it is
    the requirement's evidence, not the provider's), provider_relation, and
    relies_on_unreviewed_relation, true when no covered close or narrower concept is
    reached over accepted bridge_concept_relation edges only (every edge is proposed
    until a review workflow exists). match_depth and relies_on_unreviewed_relation
    are judged separately, so the shallowest covered concept need not be the
    reviewed one.

    A tool no course teaches, such as scikit-learn or Seaborn on the pilot, stays
    uncovered: it has no narrower concepts, because its taught neighbours (random
    forest, grid search) are methods it implements, adjudicated related, not broader.

    course_key decides which runs are the same course: dim_course.course_readable_id,
    or, when course_fk is null (edxorg runs until edxorg courses load, and courses
    missing from dim_course), courserun_course_readable_id, the run id less its last
    segment. It leaves out the platform: dim_course keys a course on
    (primary_platform, course_readable_id), so course_fk alone would let an edxorg and
    a mitxonline copy with the same readable id cover each other. Copies whose readable
    ids differ (e.g. by org) still do.

    The walk is unrolled, as in the acyclic test, because recursive CTEs are not
    portable across Trino and StarRocks; a broader cycle cannot loop it, since the
    hop count is bounded.
#}
{% set max_hops = var('courseware_prerequisite_max_narrower_hops') | int %}

with run_concepts as (
    select * from {{ ref('bridge_courserun_concept') }}
)

, concept_relations as (
    select * from {{ ref('bridge_concept_relation') }}
    where passes_support_rule
)

, courses as (
    select * from {{ ref('dim_course') }}
    where is_current
)

, course_runs as (
    select
        dim_course_run.courserun_pk
        , dim_course_run.platform
        , dim_course_run.courserun_readable_id
        , coalesce(
            courses.course_readable_id
            , {{ courserun_course_readable_id('dim_course_run.courserun_readable_id') }}
        ) as course_key
    from {{ ref('dim_course_run') }} as dim_course_run
    left join courses
        on dim_course_run.course_fk = courses.course_pk
    where dim_course_run.is_current
)

, requirements as (
    select
        run_concepts.courserun_concept_pk
        , run_concepts.platform
        , run_concepts.courserun_readable_id
        , run_concepts.courserun_fk
        , course_runs.course_key
        , run_concepts.concept_fk
        , run_concepts.requirement_source
        , run_concepts.passes_support_rule
    from run_concepts
    inner join course_runs
        on run_concepts.courserun_fk = course_runs.courserun_pk
    where run_concepts.relation = 'requires'
)

-- teaches or reviews, and whether any row teaches
, taught as (
    select
        run_concepts.courserun_fk
        , run_concepts.concept_fk
        , max(case when run_concepts.relation = 'teaches' then 1 else 0 end) = 1 as is_taught
    from run_concepts
    where run_concepts.relation in ('teaches', 'reviews')
    group by run_concepts.courserun_fk, run_concepts.concept_fk
)

-- bridge_courserun_concept leaves courserun_fk null for a run that is not current
, corpus_concepts as (
    select distinct run_concepts.concept_fk
    from run_concepts
    inner join course_runs
        on run_concepts.courserun_fk = course_runs.courserun_pk
    where run_concepts.passes_support_rule
)

, broader_edges as (
    select distinct
        from_concept_fk as narrower_concept_fk
        , to_concept_fk as broader_concept_fk
        , review_status = 'accepted' as is_reviewed
    from concept_relations
    where relation = 'broader'
)

-- close is stored once per pair, so read it both ways
, close_pairs as (
    select
        from_concept_fk as concept_fk
        , to_concept_fk as close_concept_fk
        , review_status = 'accepted' as is_reviewed
    from concept_relations
    where relation = 'close'
    union
    select
        to_concept_fk as concept_fk
        , from_concept_fk as close_concept_fk
        , review_status = 'accepted' as is_reviewed
    from concept_relations
    where relation = 'close'
)

, required_concepts as (
    select distinct concept_fk from requirements
)

-- each walk step keeps whether every edge on the path so far is accepted
, walk_1 as (
    select distinct
        required_concepts.concept_fk
        , broader_edges.narrower_concept_fk
        , broader_edges.is_reviewed
    from required_concepts
    inner join broader_edges
        on required_concepts.concept_fk = broader_edges.broader_concept_fk
)
{% for hop in range(2, max_hops + 1) %}
, walk_{{ hop }} as (
    select distinct
        walk.concept_fk
        , broader_edges.narrower_concept_fk
        , walk.is_reviewed and broader_edges.is_reviewed as is_reviewed
    from walk_{{ hop - 1 }} as walk
    inner join broader_edges
        on walk.narrower_concept_fk = broader_edges.broader_concept_fk
)
{% endfor %}

-- the fewest hops to each narrower concept, and whether any path to it is reviewed
, narrower_concepts as (
    select
        walked.concept_fk
        , walked.narrower_concept_fk
        , min(walked.hops) as hops
        , max(case when walked.is_reviewed then 1 else 0 end) = 1 as is_reviewed
    from (
        {% for hop in range(1, max_hops + 1) %}
            select concept_fk, narrower_concept_fk, is_reviewed, {{ hop }} as hops from walk_{{ hop }}
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
    ) as walked
    inner join corpus_concepts
        on walked.narrower_concept_fk = corpus_concepts.concept_fk
    -- a broader cycle leads back to the concept itself
    where walked.narrower_concept_fk != walked.concept_fk
    group by walked.concept_fk, walked.narrower_concept_fk
)

, narrower_counts as (
    select
        concept_fk
        , count(*) as narrower_concept_count
    from narrower_concepts
    group by concept_fk
)

, direct_matches as (
    select
        required_concepts.concept_fk
        , taught.courserun_fk as provider_courserun_fk
        , taught.is_taught
    from required_concepts
    inner join taught
        on required_concepts.concept_fk = taught.concept_fk
)

, close_matches as (
    select
        close_pairs.concept_fk
        , taught.courserun_fk as provider_courserun_fk
        , max(case when taught.is_taught then 1 else 0 end) = 1 as is_taught
        , max(case when close_pairs.is_reviewed then 1 else 0 end) = 1 as is_reviewed
    from close_pairs
    inner join required_concepts
        on close_pairs.concept_fk = required_concepts.concept_fk
    inner join taught
        on close_pairs.close_concept_fk = taught.concept_fk
    group by close_pairs.concept_fk, taught.courserun_fk
)

-- providers that teach or review at least one narrower concept
, narrower_matches as (
    select distinct
        narrower_concepts.concept_fk
        , taught.courserun_fk as provider_courserun_fk
    from narrower_concepts
    inner join taught
        on narrower_concepts.narrower_concept_fk = taught.concept_fk
)

, providers as (
    select concept_fk, provider_courserun_fk from direct_matches
    union
    select concept_fk, provider_courserun_fk from close_matches
    union
    select concept_fk, provider_courserun_fk from narrower_matches
)

-- every provider against every narrower concept of the requirement, covered or not
, narrower_cells as (
    select
        providers.concept_fk
        , providers.provider_courserun_fk
        , narrower_concepts.narrower_concept_fk
        , narrower_concepts.hops
        , narrower_concepts.is_reviewed
        , taught.concept_fk is not null as is_covered
    from providers
    inner join narrower_concepts
        on providers.concept_fk = narrower_concepts.concept_fk
    left join taught
        on
            providers.provider_courserun_fk = taught.courserun_fk
            and narrower_concepts.narrower_concept_fk = taught.concept_fk
)

, narrower_coverage as (
    select
        concept_fk
        , provider_courserun_fk
        , sum(case when is_covered then 1 else 0 end) as narrower_taught_count
        , min(case when is_covered then hops end) as min_covered_hops
        , max(case when is_covered and is_reviewed then 1 else 0 end) = 1 as is_reviewed
    from narrower_cells
    group by concept_fk, provider_courserun_fk
)

-- the lists are aggregated under where rather than filter, which StarRocks does not
-- allow beside order by in array_agg
, covered_narrower as (
    select
        concept_fk
        , provider_courserun_fk
        , array_agg(narrower_concept_fk order by narrower_concept_fk) as narrower_concept_fks
    from narrower_cells
    where is_covered
    group by concept_fk, provider_courserun_fk
)

, missing_narrower as (
    select
        concept_fk
        , provider_courserun_fk
        , array_agg(narrower_concept_fk order by narrower_concept_fk) as narrower_concept_fks
    from narrower_cells
    where not is_covered
    group by concept_fk, provider_courserun_fk
)

, judged as (
    select
        providers.concept_fk
        , providers.provider_courserun_fk
        , case
            when direct_matches.concept_fk is not null then 'direct'
            when close_matches.concept_fk is not null then 'close'
            else 'narrower'
        end as match_type
        , case
            when direct_matches.concept_fk is not null
                then case when direct_matches.is_taught then 'teaches' else 'reviews' end
            when close_matches.concept_fk is not null
                then case when close_matches.is_taught then 'teaches' else 'reviews' end
        end as provider_relation
        , case
            when direct_matches.concept_fk is not null or close_matches.concept_fk is not null then 0
            else narrower_coverage.min_covered_hops
        end as match_depth
        , case
            when direct_matches.concept_fk is not null then false
            when close_matches.concept_fk is not null then not close_matches.is_reviewed
            else not narrower_coverage.is_reviewed
        end as relies_on_unreviewed_relation
        , coalesce(narrower_counts.narrower_concept_count, 0) as narrower_concept_count
        , coalesce(narrower_coverage.narrower_taught_count, 0) as narrower_taught_count
        , covered_narrower.narrower_concept_fks as covered_narrower_concept_fks
        , missing_narrower.narrower_concept_fks as missing_narrower_concept_fks
    from providers
    left join direct_matches
        on
            providers.concept_fk = direct_matches.concept_fk
            and providers.provider_courserun_fk = direct_matches.provider_courserun_fk
    left join close_matches
        on
            providers.concept_fk = close_matches.concept_fk
            and providers.provider_courserun_fk = close_matches.provider_courserun_fk
    left join narrower_counts
        on providers.concept_fk = narrower_counts.concept_fk
    left join narrower_coverage
        on
            providers.concept_fk = narrower_coverage.concept_fk
            and providers.provider_courserun_fk = narrower_coverage.provider_courserun_fk
    left join covered_narrower
        on
            providers.concept_fk = covered_narrower.concept_fk
            and providers.provider_courserun_fk = covered_narrower.provider_courserun_fk
    left join missing_narrower
        on
            providers.concept_fk = missing_narrower.concept_fk
            and providers.provider_courserun_fk = missing_narrower.provider_courserun_fk
)

select
    {{ dbt_utils.generate_surrogate_key(['requirements.courserun_concept_pk', 'judged.provider_courserun_fk']) }}
    as courserun_prerequisite_coverage_pk
    , requirements.courserun_concept_pk as courserun_concept_fk
    , requirements.platform
    , requirements.courserun_readable_id
    , requirements.courserun_fk
    , requirements.concept_fk
    , requirements.requirement_source
    , requirements.passes_support_rule as requirement_passes_support_rule
    , provider_runs.platform as provider_platform
    , provider_runs.courserun_readable_id as provider_courserun_readable_id
    , judged.provider_courserun_fk
    , judged.match_type
    , judged.provider_relation
    , judged.match_depth
    , judged.relies_on_unreviewed_relation
    , judged.narrower_concept_count
    , judged.narrower_taught_count
    , judged.covered_narrower_concept_fks
    , judged.missing_narrower_concept_fks
from requirements
inner join judged
    on requirements.concept_fk = judged.concept_fk
inner join course_runs as provider_runs
    on judged.provider_courserun_fk = provider_runs.courserun_pk
where requirements.course_key != provider_runs.course_key
