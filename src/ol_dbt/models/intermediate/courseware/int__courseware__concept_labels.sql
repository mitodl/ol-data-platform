-- One row per normalized concept label across every course: its most used surface
-- form, kind and a description. This is the list concept resolution reads.
with mentions as (
    select * from {{ ref('int__courseware__concept_mentions') }}
)

, label_counts as (
    select
        concept_label_key
        , label
        , count(*) as mention_count
    from mentions
    group by concept_label_key, label
)

, preferred_labels as (
    select
        concept_label_key
        , label as preferred_label
    from (
        select
            *
            , row_number() over (
                partition by concept_label_key order by mention_count desc, label ASC
            ) as label_rank
        from label_counts
    ) as ranked
    where label_rank = 1
)

, kinds as (
    select
        concept_label_key
        , concept_kind
    from (
        select
            concept_label_key
            , concept_kind
            , row_number() over (
                partition by concept_label_key order by count(*) desc, concept_kind
            ) as kind_rank
        from mentions
        where concept_kind is not null
        group by concept_label_key, concept_kind
    ) as ranked
    where kind_rank = 1
)

-- a taught description says what the concept is better than a used one
, descriptions as (
    select
        concept_label_key
        , description
    from (
        select
            concept_label_key
            , description
            , row_number() over (
                partition by concept_label_key
                order by case when teaching_status = 'taught' then 0 else 1 end, length(description) desc
            ) as description_rank
        from mentions
        where description is not null
    ) as ranked
    where description_rank = 1
)

, label_stats as (
    select
        concept_label_key
        , count(*) as mention_count
        , count(distinct concat(platform, '|', courserun_readable_id)) as courserun_count
        , count(case when teaching_status in ('taught', 'reviewed') then 1 end) as taught_mention_count
    from mentions
    group by concept_label_key
)

select
    label_stats.concept_label_key
    , preferred_labels.preferred_label
    , kinds.concept_kind
    , descriptions.description
    , label_stats.mention_count
    , label_stats.courserun_count
    , label_stats.taught_mention_count
from label_stats
inner join preferred_labels
    on label_stats.concept_label_key = preferred_labels.concept_label_key
left join kinds
    on label_stats.concept_label_key = kinds.concept_label_key
left join descriptions
    on label_stats.concept_label_key = descriptions.concept_label_key
