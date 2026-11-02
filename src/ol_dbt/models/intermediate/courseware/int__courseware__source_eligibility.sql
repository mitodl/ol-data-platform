{#
    Whether what each extracted course run's material says may reach a
    learner-facing projection, under courseware_source_eligibility. A run takes its
    own row, else its platform's, and a run neither names is not eligible.

    source_policy_version is a checksum of the whole policy, the same on every row,
    so a knowledge release records the policy it was built under and a policy edit
    builds a new release.
#}
with policy as (
    select * from {{ ref('courseware_source_eligibility') }}
)

, policy_version as (
    select
        {{ md5_hex(array_join(
            "array_agg(concat_ws('|', platform, coalesce(courserun_readable_id, ''),"
            ~ " case when is_eligible then 'eligible' else 'ineligible' end)"
            ~ " order by platform, coalesce(courserun_readable_id, ''))", ','
        )) }} as source_policy_version
    from policy
)

, runs as (
    select distinct
        platform
        , courserun_readable_id
    from {{ ref('int__courseware__extractions') }}
)

select
    runs.platform
    , runs.courserun_readable_id
    , coalesce(run_policy.is_eligible, platform_policy.is_eligible, false) as is_source_eligible
    , case
        when run_policy.platform is not null then 'courserun'
        when platform_policy.platform is not null then 'platform'
    end as eligibility_level
    , policy_version.source_policy_version
from runs
left join policy as run_policy
    on
        runs.platform = run_policy.platform
        and runs.courserun_readable_id = run_policy.courserun_readable_id
left join policy as platform_policy
    on
        runs.platform = platform_policy.platform
        and platform_policy.courserun_readable_id is null
cross join policy_version
