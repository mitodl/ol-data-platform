{{ config(
    materialized='table'
) }}

-- Build hierarchical topic taxonomy from all platforms
with mitxonline_topics as (
    select distinct
        coursetopic_id as topic_id
        , coursetopic_name as topic_name
        , coursetopic_parent_id as parent_topic_id
        , '{{ var("mitxonline") }}' as platform
    from {{ ref('int__mitxonline__course_to_topics') }}
)

, mitxpro_topics as (
    select distinct
        coursetopic_id as topic_id
        , coursetopic_name as topic_name
        , coursetopic_parent_coursetopic_id as parent_topic_id
        , '{{ var("mitxpro") }}' as platform
    from {{ ref('int__mitxpro__courses_to_topics') }}
)

-- OCW has 3-level hierarchy: topic > subtopic > speciality
, ocw_topics as (
    select distinct
        cast(null as bigint) as topic_id
        , course_topic as topic_name
        , cast(null as bigint) as parent_topic_id
        , 'ocw' as platform
    from {{ ref('int__ocw__course_topics') }}
    where course_topic is not null

    union all

    select distinct
        cast(null as bigint) as topic_id
        , course_subtopic as topic_name
        , cast(null as bigint) as parent_topic_id  -- Would need to link to parent topic
        , 'ocw' as platform
    from {{ ref('int__ocw__course_topics') }}
    where course_subtopic is not null

    union all

    select distinct
        cast(null as bigint) as topic_id
        , course_speciality as topic_name
        , cast(null as bigint) as parent_topic_id  -- Would need to link to parent subtopic
        , 'ocw' as platform
    from {{ ref('int__ocw__course_topics') }}
    where course_speciality is not null
)

, combined_topics as (
    select * from mitxonline_topics
    union all
    select * from mitxpro_topics
    union all
    select * from ocw_topics
)

-- Deduplicate by topic_name (topics can exist across platforms)
, deduped_topics as (
    select
        topic_name
        , max(topic_id) as source_topic_id
        , max(parent_topic_id) as source_parent_topic_id
        , min(platform) as primary_platform
    from combined_topics
    where topic_name is not null
    group by topic_name
)

, deduped_with_pk as (
    select
        {{ dbt_utils.generate_surrogate_key(['topic_name']) }} as topic_pk
        , topic_name
        , source_topic_id
        , source_parent_topic_id
        , primary_platform
    from deduped_topics
)

-- Resolve parent_topic_fk via self-join on source topic IDs
, with_parent_fk as (
    select
        child.topic_pk
        , child.topic_name
        , child.source_topic_id
        , child.source_parent_topic_id
        , parent.topic_pk as parent_topic_fk
        , child.primary_platform
    from deduped_with_pk as child
    left join deduped_with_pk as parent
        on child.source_parent_topic_id = parent.source_topic_id
        and child.source_parent_topic_id is not null
)

select
    topic_pk
    , topic_name
    , source_topic_id
    , source_parent_topic_id
    , parent_topic_fk
    , primary_platform
from with_parent_fk
