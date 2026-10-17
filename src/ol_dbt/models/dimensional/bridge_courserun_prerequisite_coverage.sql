{#
    Which other course runs cover each concept a course run requires, one row per
    (requirement, providing run). A requirement is a requires row of
    bridge_courserun_concept, declared or inferred, published or not; filter on
    requirement_is_published as the Learn views do. Requirements no other run covers
    have no rows.

    A providing run is a current run of a different course, and match_type is the
    first of these that applies (see course_key below for "different course"):
      direct    it teaches or reviews the concept
      close     it teaches or reviews a close equivalent (bridge_concept_relation
                close: nearly interchangeable, not merged)
      narrower  it teaches or reviews enough of the concept's narrower concepts

    narrower is for umbrella requirements such as "Python programming", which courses
    teach only as their parts (lists, functions, modules). The narrower concepts of a
    required concept are those reachable down published broader edges in at most
    courseware_prerequisite_max_narrower_hops edges that have any published row in
    bridge_courserun_concept, i.e. appear in some current course run.
    narrower_coverage is narrower_taught_count, those the providing run teaches or
    reviews, over narrower_concept_count. It satisfies the requirement at
    courseware_prerequisite_min_narrower_coverage or more with at least
    courseware_prerequisite_min_narrower_taught concepts taught, because a ratio over
    one or two parts says little. The walk does not start from close equivalents too:
    on the pilot that pulled the mathematical "function" tree (logistic, softmax) under
    "Python function".

    A tool no course teaches, such as scikit-learn or Seaborn on the pilot, stays
    uncovered: it has no narrower concepts, because its taught neighbours (random
    forest, grid search) are methods it implements, adjudicated related, not broader,
    and every pilot course that uses it also requires it.

    course_key decides which runs are the same course: dim_course.course_readable_id,
    or, when course_fk is null (edxorg runs until edxorg courses load, and courses
    missing from dim_course), the run id less its last segment, as dim_course_run
    derives the id it joins dim_course on. extract_course_readable_id is not used
    because it stops at an underscore (MITx+15.390.1x_KOR becomes MITx+15.390.1x)
    and returns 'course-v1:' for ids in neither format. It leaves out the platform:
    dim_course keys a course on (primary_platform, course_readable_id), so course_fk
    alone would let an edxorg and a mitxonline copy with the same readable id cover
    each other. Copies whose readable ids differ (e.g. by org) still do.

    Rows below the thresholds stay, with is_satisfied false, so the thresholds can be
    reviewed. The walk is unrolled, as in the acyclic test, because recursive CTEs are
    not portable across Trino and StarRocks; a broader cycle cannot loop it, since the
    hop count is bounded.
#}
{% set max_hops = var('courseware_prerequisite_max_narrower_hops') | int %}

with run_concepts as (
    select * from {{ ref('bridge_courserun_concept') }}
)

, concept_relations as (
    select * from {{ ref('bridge_concept_relation') }}
    where is_published
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
            , case
                when dim_course_run.courserun_readable_id like 'course-v1:%'
                    then substring(
                        dim_course_run.courserun_readable_id
                        , 1
                        , length(dim_course_run.courserun_readable_id)
                        - strpos(reverse(dim_course_run.courserun_readable_id), '+')
                    )
                when {{ regexp_like('dim_course_run.courserun_readable_id', "'^[^/]+/[^/]+/[^/]+'") }}
                    then substring(
                        dim_course_run.courserun_readable_id
                        , 1
                        , length(dim_course_run.courserun_readable_id)
                        - strpos(reverse(dim_course_run.courserun_readable_id), '/')
                    )
                else dim_course_run.courserun_readable_id
            end
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
        , run_concepts.is_published
    from run_concepts
    inner join course_runs
        on run_concepts.courserun_fk = course_runs.courserun_pk
    where run_concepts.relation = 'requires'
)

, taught as (
    select distinct
        run_concepts.courserun_fk
        , run_concepts.concept_fk
    from run_concepts
    where run_concepts.relation in ('teaches', 'reviews')
)

-- bridge_courserun_concept leaves courserun_fk null for a run that is not current
, corpus_concepts as (
    select distinct run_concepts.concept_fk
    from run_concepts
    inner join course_runs
        on run_concepts.courserun_fk = course_runs.courserun_pk
    where run_concepts.is_published
)

, broader_edges as (
    select distinct
        from_concept_fk as narrower_concept_fk
        , to_concept_fk as broader_concept_fk
    from concept_relations
    where relation = 'broader'
)

-- close is stored once per pair, so read it both ways
, close_pairs as (
    select
        from_concept_fk as concept_fk
        , to_concept_fk as close_concept_fk
    from concept_relations
    where relation = 'close'
    union
    select
        to_concept_fk as concept_fk
        , from_concept_fk as close_concept_fk
    from concept_relations
    where relation = 'close'
)

, required_concepts as (
    select distinct concept_fk from requirements
)

, walk_1 as (
    select distinct
        required_concepts.concept_fk
        , broader_edges.narrower_concept_fk
    from required_concepts
    inner join broader_edges
        on required_concepts.concept_fk = broader_edges.broader_concept_fk
)
{% for hop in range(2, max_hops + 1) %}
, walk_{{ hop }} as (
    select distinct
        walk.concept_fk
        , broader_edges.narrower_concept_fk
    from walk_{{ hop - 1 }} as walk
    inner join broader_edges
        on walk.narrower_concept_fk = broader_edges.broader_concept_fk
)
{% endfor %}

, narrower_concepts as (
    select distinct
        walked.concept_fk
        , walked.narrower_concept_fk
    from (
        {% for hop in range(1, max_hops + 1) %}
            select concept_fk, narrower_concept_fk from walk_{{ hop }}
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
    ) as walked
    inner join corpus_concepts
        on walked.narrower_concept_fk = corpus_concepts.concept_fk
    -- a broader cycle leads back to the concept itself
    where walked.narrower_concept_fk != walked.concept_fk
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
    from required_concepts
    inner join taught
        on required_concepts.concept_fk = taught.concept_fk
)

, close_matches as (
    select distinct
        close_pairs.concept_fk
        , taught.courserun_fk as provider_courserun_fk
    from close_pairs
    inner join required_concepts
        on close_pairs.concept_fk = required_concepts.concept_fk
    inner join taught
        on close_pairs.close_concept_fk = taught.concept_fk
)

, narrower_matches as (
    select
        narrower_concepts.concept_fk
        , taught.courserun_fk as provider_courserun_fk
        , count(*) as narrower_taught_count
    from narrower_concepts
    inner join taught
        on narrower_concepts.narrower_concept_fk = taught.concept_fk
    group by narrower_concepts.concept_fk, taught.courserun_fk
)

, providers as (
    select concept_fk, provider_courserun_fk from direct_matches
    union
    select concept_fk, provider_courserun_fk from close_matches
    union
    select concept_fk, provider_courserun_fk from narrower_matches
)

, judged as (
    select
        providers.concept_fk
        , providers.provider_courserun_fk
        , direct_matches.concept_fk is not null as is_direct
        , close_matches.concept_fk is not null as is_close
        , coalesce(narrower_counts.narrower_concept_count, 0) as narrower_concept_count
        , coalesce(narrower_matches.narrower_taught_count, 0) as narrower_taught_count
        , cast(coalesce(narrower_matches.narrower_taught_count, 0) as double)
        / narrower_counts.narrower_concept_count as narrower_coverage
        , coalesce(
            narrower_matches.narrower_taught_count >= {{ var('courseware_prerequisite_min_narrower_taught') }}
            and cast(narrower_matches.narrower_taught_count as double) / narrower_counts.narrower_concept_count
            >= {{ var('courseware_prerequisite_min_narrower_coverage') }}
            , false
        ) as is_narrower_satisfied
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
    left join narrower_matches
        on
            providers.concept_fk = narrower_matches.concept_fk
            and providers.provider_courserun_fk = narrower_matches.provider_courserun_fk
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
    , requirements.is_published as requirement_is_published
    , provider_runs.platform as provider_platform
    , provider_runs.courserun_readable_id as provider_courserun_readable_id
    , judged.provider_courserun_fk
    , case
        when judged.is_direct then 'direct'
        when judged.is_close then 'close'
        else 'narrower'
    end as match_type
    , judged.narrower_concept_count
    , judged.narrower_taught_count
    , judged.narrower_coverage
    , judged.is_direct or judged.is_close or judged.is_narrower_satisfied as is_satisfied
from requirements
inner join judged
    on requirements.concept_fk = judged.concept_fk
inner join course_runs as provider_runs
    on judged.provider_courserun_fk = provider_runs.courserun_pk
where requirements.course_key != provider_runs.course_key
