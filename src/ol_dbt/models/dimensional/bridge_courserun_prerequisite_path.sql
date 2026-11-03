{#
    Experimental preparation candidates for a course run, not validated prerequisites
    or readiness advice: every concept it requires (inferred requirements are
    observations of the run), and what those build on through
    bridge_concept_prerequisite_closure, with the runs of other courses that teach
    each one. One row per (run, prerequisite concept, providing run), and one row with
    a null provider for a prerequisite no other course covers. Only requirements of
    current runs that pass the support rule count. The closure reads approved
    builds_on edges only, so until a review workflow approves some, every row is at
    depth 1.

    prerequisite_depth is 1 for a concept the run requires, and 1 plus the
    bridge_concept_prerequisite_closure depth for a concept a requirement builds on
    (the shortest over the run's requirements). prerequisite_source is declared or
    inferred at depth 1, builds_on deeper. A concept reached through builds_on that
    the run teaches or reviews itself is left out, since the run covers it.
    required_concept_count is how many of the run's requirements are or build on the
    concept, so a concept many requirements lead to ranks as more central.

    Providers follow bridge_courserun_prerequisite_coverage: a current run of a
    different course (courseware_current_course_runs) that teaches or reviews the
    concept (direct) or a close equivalent (close). Depth 1 also takes coverage's
    narrower matches, for umbrella requirements, ranked below direct and close by how
    many narrower concepts they cover; coverage gives no verdict on whether that is
    enough. provider_scope_count is the
    provider's scopes teaching or reviewing the concept (direct) or its best close
    equivalent (close), a scope that does both counted once; a narrower match counts concepts instead, so it has
    narrower_taught_count from coverage and a null scope count. provider_rank orders
    a prerequisite's providers by match type, then by whichever of the two the match
    type has. provider_prerequisite_count
    is how many of the run's prerequisites the provider covers, which ranks candidate
    courses.
#}
with run_concepts as (
    select * from {{ ref('bridge_courserun_concept') }}
    where courserun_fk is not null and passes_support_rule
)

, closure as (
    select * from {{ ref('bridge_concept_prerequisite_closure') }}
)

, course_runs as (
    {{ courseware_current_course_runs() }}
)

, close_pairs as (
    {{ courseware_close_concept_pairs() }}
)

, requirements as (
    select
        courserun_fk
        , concept_fk
        , max(case when requirement_source = 'declared' then 1 else 0 end) = 1 as is_declared
    from run_concepts
    where relation = 'requires'
    group by courserun_fk, concept_fk
)

-- what each run teaches or reviews, read from the claims bridge_courserun_concept
-- reads because its teaches and reviews rows each count their scopes, so summing them
-- would count a scope that both teaches and reviews a concept twice
, taught as (
    select
        course_runs.courserun_pk as courserun_fk
        , mentions.concept_id as concept_fk
        , count(distinct mentions.scope_id) as scope_count
    from {{ ref('int__courseware__resolved_concept_mentions') }} as mentions
    inner join course_runs
        on
            mentions.platform = course_runs.platform
            and mentions.courserun_readable_id = course_runs.courserun_readable_id
    where
        mentions.claim_kind = 'concept'
        and mentions.teaching_status in ('taught', 'reviewed')
    group by course_runs.courserun_pk, mentions.concept_id
)

-- each requirement reaches itself at depth 0 and what it builds on at closure depth
, reached as (
    select
        courserun_fk
        , concept_fk as required_concept_fk
        , concept_fk
        , 0 as closure_depth
        , is_declared
    from requirements
    union all
    select
        requirements.courserun_fk
        , requirements.concept_fk as required_concept_fk
        , closure.to_concept_fk as concept_fk
        , closure.min_depth as closure_depth
        , false as is_declared
    from requirements
    inner join closure
        on requirements.concept_fk = closure.from_concept_fk
)

, prerequisites as (
    select
        reached.courserun_fk
        , reached.concept_fk
        , min(reached.closure_depth) + 1 as prerequisite_depth
        , case
            when max(case when reached.closure_depth = 0 and reached.is_declared then 1 else 0 end) = 1
                then 'declared'
            when min(reached.closure_depth) = 0 then 'inferred'
            else 'builds_on'
        end as prerequisite_source
        , count(distinct reached.required_concept_fk) as required_concept_count
    from reached
    left join taught
        on
            reached.courserun_fk = taught.courserun_fk
            and reached.concept_fk = taught.concept_fk
    group by reached.courserun_fk, reached.concept_fk
    -- a concept the run teaches is a prerequisite only when the run requires it
    having min(reached.closure_depth) = 0 or max(taught.concept_fk) is null
)

, direct_matches as (
    select
        prerequisites.courserun_fk
        , prerequisites.concept_fk
        , taught.courserun_fk as provider_courserun_fk
        , 'direct' as match_type
        , taught.scope_count as provider_scope_count
        , cast(null as bigint) as narrower_taught_count
    from prerequisites
    inner join taught
        on prerequisites.concept_fk = taught.concept_fk
)

, close_matches as (
    select
        prerequisites.courserun_fk
        , prerequisites.concept_fk
        , taught.courserun_fk as provider_courserun_fk
        , 'close' as match_type
        , max(taught.scope_count) as provider_scope_count
        , cast(null as bigint) as narrower_taught_count
    from prerequisites
    inner join close_pairs
        on prerequisites.concept_fk = close_pairs.concept_fk
    inner join taught
        on close_pairs.close_concept_fk = taught.concept_fk
    group by prerequisites.courserun_fk, prerequisites.concept_fk, taught.courserun_fk
)

, narrower_matches as (
    select
        courserun_fk
        , concept_fk
        , provider_courserun_fk
        , 'narrower' as match_type
        , cast(null as bigint) as provider_scope_count
        , narrower_taught_count
    from {{ ref('bridge_courserun_prerequisite_coverage') }}
    where
        requirement_passes_support_rule
        and match_type = 'narrower'
)

, candidate_matches as (
    select * from direct_matches
    union all
    select * from close_matches
    union all
    select * from narrower_matches
)

-- the first match type that applies, as in bridge_courserun_prerequisite_coverage
, providers as (
    select
        candidate_matches.courserun_fk
        , candidate_matches.concept_fk
        , candidate_matches.provider_courserun_fk
        , candidate_matches.match_type
        , candidate_matches.provider_scope_count
        , candidate_matches.narrower_taught_count
        , provider_runs.platform as provider_platform
        , provider_runs.courserun_readable_id as provider_courserun_readable_id
        , row_number() over (
            partition by
                candidate_matches.courserun_fk
                , candidate_matches.concept_fk
                , candidate_matches.provider_courserun_fk
            order by
                case candidate_matches.match_type when 'direct' then 1 when 'close' then 2 else 3 end
        ) as match_rank
    from candidate_matches
    inner join course_runs as requiring_runs
        on candidate_matches.courserun_fk = requiring_runs.courserun_pk
    inner join course_runs as provider_runs
        on candidate_matches.provider_courserun_fk = provider_runs.courserun_pk
    where requiring_runs.course_key != provider_runs.course_key
)

, paths as (
    select
        prerequisites.courserun_fk
        , prerequisites.concept_fk
        , prerequisites.prerequisite_depth
        , prerequisites.prerequisite_source
        , prerequisites.required_concept_count
        , providers.provider_platform
        , providers.provider_courserun_readable_id
        , providers.provider_courserun_fk
        , providers.match_type
        , providers.provider_scope_count
        , providers.narrower_taught_count
    from prerequisites
    left join providers
        on
            prerequisites.courserun_fk = providers.courserun_fk
            and prerequisites.concept_fk = providers.concept_fk
            and providers.match_rank = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['paths.courserun_fk', 'paths.concept_fk', 'paths.provider_courserun_fk']) }}
    as courserun_prerequisite_path_pk
    , course_runs.platform
    , course_runs.courserun_readable_id
    , paths.courserun_fk
    , paths.concept_fk
    , paths.prerequisite_depth
    , paths.prerequisite_source
    , paths.required_concept_count
    , paths.provider_platform
    , paths.provider_courserun_readable_id
    , paths.provider_courserun_fk
    , paths.match_type
    , paths.provider_scope_count
    , paths.narrower_taught_count
    , case
        when paths.provider_courserun_fk is not null
            then row_number() over (
                partition by paths.courserun_fk, paths.concept_fk
                order by
                    case paths.match_type when 'direct' then 1 when 'close' then 2 else 3 end
                    , coalesce(paths.provider_scope_count, paths.narrower_taught_count) desc
                    , paths.provider_courserun_readable_id asc
            )
    end as provider_rank
    , case
        when paths.provider_courserun_fk is not null
            then count(*) over (partition by paths.courserun_fk, paths.provider_courserun_fk)
    end as provider_prerequisite_count
from paths
inner join course_runs
    on paths.courserun_fk = course_runs.courserun_pk
