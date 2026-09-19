{{ config(error_if="!=0") }}

-- The learn_topics / learn_topic_offeror_mappings seeds are the data platform's copy of
-- MIT Learn's topic taxonomy. Until the MIT Learn ETL cutover, MIT Learn's data_fixtures
-- migrations are still what writes the taxonomy, so every row here is drift between the
-- seeds and MIT Learn's replicated tables: `drift` says which side is missing the row.
-- Fix by updating the seed to match MIT Learn. A row only on the MIT Learn side can also
-- be one MIT Learn deleted that its replicated table still holds; check MIT Learn before
-- adding it to the seed. After cutover the direction flips (MIT Learn consumes the seed)
-- and this test should be removed.
with seed_topics as (
    select
        cast(topic_uuid as varchar) as topic_uuid
        , topic_name
        , cast(parent_topic_uuid as varchar) as parent_topic_uuid
    from {{ ref('learn_topics') }}
)

, mitlearn_topics as (
    select
        cast(topic.learningresourcetopic_uuid as varchar) as topic_uuid
        , topic.learningresourcetopic_name as topic_name
        , cast(parent.learningresourcetopic_uuid as varchar) as parent_topic_uuid
    from {{ ref('stg__mitlearn__app__postgres__learning_resources_learningresourcetopic') }} as topic
    left join {{ ref('stg__mitlearn__app__postgres__learning_resources_learningresourcetopic') }} as parent
        on topic.learningresourcetopic_parent_id = parent.learningresourcetopic_id
)

, seed_mappings as (
    select
        offeror_code
        , offeror_topic_name
        , cast(topic_uuid as varchar) as topic_uuid
    from {{ ref('learn_topic_offeror_mappings') }}
)

, mitlearn_mappings as (
    select
        mapping.learningresourceofferor_code as offeror_code
        , mapping.learningresourcetopicmapping_offeror_topic_name as offeror_topic_name
        , cast(topic.learningresourcetopic_uuid as varchar) as topic_uuid
    from {{ ref('stg__mitlearn__app__postgres__learning_resources_learningresourcetopicmapping') }} as mapping
    inner join {{ ref('stg__mitlearn__app__postgres__learning_resources_learningresourcetopic') }} as topic
        on mapping.learningresourcetopic_id = topic.learningresourcetopic_id
)

, topics_missing_from_seed as (
    select * from mitlearn_topics
    except
    select * from seed_topics
)

, topics_missing_from_mitlearn as (
    select * from seed_topics
    except
    select * from mitlearn_topics
)

, mappings_missing_from_seed as (
    select * from mitlearn_mappings
    except
    select * from seed_mappings
)

, mappings_missing_from_mitlearn as (
    select * from seed_mappings
    except
    select * from mitlearn_mappings
)

select
    'topic missing from seed' as drift
    , topic_uuid
    , topic_name as name
    , parent_topic_uuid as detail
from topics_missing_from_seed

union all

select
    'topic missing from mit-learn' as drift
    , topic_uuid
    , topic_name as name
    , parent_topic_uuid as detail
from topics_missing_from_mitlearn

union all

select
    'mapping missing from seed' as drift
    , topic_uuid
    , offeror_topic_name as name
    , offeror_code as detail
from mappings_missing_from_seed

union all

select
    'mapping missing from mit-learn' as drift
    , topic_uuid
    , offeror_topic_name as name
    , offeror_code as detail
from mappings_missing_from_mitlearn
