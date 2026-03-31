{{ config(
    materialized='table'
) }}

-- Build hierarchical topic taxonomy from all platforms
with mitxonline_topics_raw as (
    select distinct
        coursetopic_id as topic_id
        , coursetopic_name as topic_name
        , coursetopic_parent_id as parent_topic_id
        , 'mitxonline' as platform
    from {{ ref('int__mitxonline__course_to_topics') }}
)

, mitxpro_topics_raw as (
    select distinct
        coursetopic_id as topic_id
        , coursetopic_name as topic_name
        , coursetopic_parent_coursetopic_id as parent_topic_id
        , 'mitxpro' as platform
    from {{ ref('int__mitxpro__courses_to_topics') }}
)

-- Resolve parent topic NAME within each platform before cross-platform dedup.
-- Using names (not IDs) for parent resolution avoids cross-platform ID collisions.
, mitxonline_topics as (
    select
        c.topic_name
        , p.topic_name as parent_topic_name
        , c.platform
    from mitxonline_topics_raw as c
    left join mitxonline_topics_raw as p
        on c.parent_topic_id = p.topic_id
)

, mitxpro_topics as (
    select
        c.topic_name
        , p.topic_name as parent_topic_name
        , c.platform
    from mitxpro_topics_raw as c
    left join mitxpro_topics_raw as p
        on c.parent_topic_id = p.topic_id
)

-- OCW has 3-level hierarchy: topic > subtopic > speciality
, ocw_topics as (
    select distinct
        course_topic as topic_name
        , cast(null as varchar) as parent_topic_name
        , 'ocw' as platform
    from {{ ref('int__ocw__course_topics') }}
    where course_topic is not null

    union all

    select distinct
        course_subtopic as topic_name
        , course_topic as parent_topic_name
        , 'ocw' as platform
    from {{ ref('int__ocw__course_topics') }}
    where course_subtopic is not null

    union all

    select distinct
        course_speciality as topic_name
        , course_subtopic as parent_topic_name
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

-- Keep per-platform rows — same topic name from different platforms are tracked separately.
-- Parent-child resolution is done within-platform before dedup.
, deduped_topics as (
    select
        topic_name
        , min(parent_topic_name) as parent_topic_name
        , platform as primary_platform
    from combined_topics
    where topic_name is not null
    group by topic_name, platform
)

, deduped_with_pk as (
    select
        {{ dbt_utils.generate_surrogate_key(['topic_name', 'primary_platform']) }} as topic_pk
        , topic_name
        , parent_topic_name
        , primary_platform
    from deduped_topics
)

-- Resolve parent_topic_fk within the same platform
, with_parent_fk as (
    select
        child.topic_pk
        , child.topic_name
        , child.parent_topic_name
        , parent.topic_pk as parent_topic_fk
        , child.primary_platform
    from deduped_with_pk as child
    left join deduped_with_pk as parent
        on child.parent_topic_name = parent.topic_name
        and child.primary_platform = parent.primary_platform
        and child.parent_topic_name is not null
)

select
    topic_pk
    , topic_name
    , parent_topic_name
    , parent_topic_fk
    , primary_platform
from with_parent_fk
