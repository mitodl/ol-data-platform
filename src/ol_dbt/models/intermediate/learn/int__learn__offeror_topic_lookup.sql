{#
  Resolves an offeror's own topic names (e.g. edX subjects) to MIT Learn topics.
  Join on (offeror_code, offeror_topic_name); a name with no row here is not an MIT
  Learn topic for that offeror and is dropped.

  Encodes the two rules MIT Learn's transform_topics applied:
  1. A name with explicit mappings for the offeror resolves to every mapped topic.
  2. Otherwise the name resolves to itself if it is exactly an MIT Learn topic name.
#}

with topics as (
    select * from {{ ref('learn_topics') }}
)

, mappings as (
    select * from {{ ref('learn_topic_offeror_mappings') }}
)

, explicit_mappings as (
    select
        mappings.offeror_code
        , mappings.offeror_topic_name
        , topics.topic_uuid
        , topics.topic_name
    from mappings
    inner join topics on mappings.topic_uuid = topics.topic_uuid
)

, offerors as (
    select distinct offeror_code from mappings
)

, same_name_topics as (
    select
        offerors.offeror_code
        , topics.topic_name as offeror_topic_name
        , topics.topic_uuid
        , topics.topic_name
    from offerors
    cross join topics
    where not exists (
        select 1
        from mappings
        where
            mappings.offeror_code = offerors.offeror_code
            and mappings.offeror_topic_name = topics.topic_name
    )
)

select * from explicit_mappings
union all
select * from same_name_topics
