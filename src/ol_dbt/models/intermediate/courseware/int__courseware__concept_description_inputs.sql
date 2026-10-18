{#
    What the concept describer reads for each concept: its labels, and up to five
    claim descriptions from different course runs (taught ones first) with the
    course each came from. The claim descriptions say how one scope treats the
    concept; the describer turns them into one course-agnostic description, and the
    course titles let it disambiguate labels like "kernel".

    Every row carries the description prompt version courseware_description_approval
    approves, so the describer refuses any other prompt, and this model's first build
    after a promotion requests a description run. A seed with more than one row
    approves nothing rather than repeating every row, which would change every
    concept's input checksum and describe the corpus again.
#}
with mentions as (
    select * from {{ ref('int__courseware__resolved_concept_mentions') }}
    where description is not null
)

, approval as (
    select case when count(*) = 1 then max(prompt_version) end as prompt_version
    from {{ ref('courseware_description_approval') }}
)

, course_runs as (
    select * from {{ ref('dim_course_run') }}
    where is_current
)

, courses as (
    select * from {{ ref('dim_course') }}
    where is_current
)

, ranked as (
    select
        mentions.concept_id
        , mentions.label
        , mentions.concept_kind
        , mentions.description
        , courses.course_title
        , row_number() over (
            partition by mentions.concept_id, mentions.platform, mentions.courserun_readable_id
            order by case when mentions.teaching_status = 'taught' then 0 else 1 end, length(mentions.description) desc
        ) as run_rank
    from mentions
    left join course_runs
        on
            mentions.platform = course_runs.platform
            and mentions.courserun_readable_id = course_runs.courserun_readable_id
    left join courses
        on course_runs.course_fk = courses.course_pk
)

, one_per_run as (
    select
        concept_id
        , label
        , concept_kind
        , description
        , course_title
        , row_number() over (partition by concept_id order by course_title, label) as context_rank
    from ranked
    where run_rank = 1
)

select
    one_per_run.*
    , approval.prompt_version as approved_prompt_version
from one_per_run
cross join approval
where one_per_run.context_rank <= 5
