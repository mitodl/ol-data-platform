{#
  int__learn__offeror_topics, gated on the topic seeds matching MIT Learn.

  The staging refs below are not read. dbt only lets a multi-parent test gate a model
  that descends from all of the test's parents, so they make
  assert_learn_topic_seeds_match_mitlearn block this model (and delivery) on drift.
#}
-- depends_on: {{ ref('stg__mitlearn__app__postgres__learning_resources_learningresourcetopic') }}
-- depends_on: {{ ref('stg__mitlearn__app__postgres__learning_resources_learningresourcetopicmapping') }}

select * from {{ ref('int__learn__offeror_topics') }}
