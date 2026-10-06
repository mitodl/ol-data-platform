{#
  Shared bodies of the b2b_analytics engagement views (StarRocks only). The org-grain and
  contract-grain views differ only in what they group by, so the rows they aggregate are
  defined once here. Every row is a learner enrolled in a course run
  (afact_learner_courserun_progress), keyed on user_fk, with activity read from
  afact_learner_courserun_daily_activity as the learner-records views read it.
#}

{# course run -> contract -> organization. A course run belongs to exactly one B2B
   contract (courses_courserun.b2b_contract_id), so joining on courserun_pk cannot fan out. #}
{% macro b2b_contract_courseruns() %}
    select
        cr.courserun_pk,
        cr.courserun_readable_id,
        cr.courserun_title,
        c.contract_pk,
        c.contract_id,
        c.b2b_contract_name,
        org.organization_key,
        org.sso_organization_id,
        org.organization_name
    from {{ source('dimensional', 'bridge_organization_courserun') }} boc
    join {{ source('dimensional', 'dim_contract') }} c
        on boc.contract_fk = c.contract_pk
    join {{ source('dimensional', 'dim_organization') }} org
        on c.organization_fk = org.organization_pk
    join {{ source('dimensional', 'dim_course_run') }} cr
        on boc.courserun_fk = cr.courserun_pk
    where org.platform = 'mitxonline'
      and cr.is_current = true
{% endmacro %}

{# One row per (learner, course run, month, kind of thing that happened in it): a day of
   tracked activity, the enrollment, or the certificate. is_active_day is set on activity
   rows only, so enrolling or being issued a certificate does not make a learner active
   in a month. The month of an activity day is sliced from activity_date_key (YYYYMMDD);
   the enrollment and certificate months are sliced from their ISO-8601 strings. #}
{% macro b2b_learner_courserun_months() %}
    select
        a.user_fk,
        a.courserun_fk,
        concat(
            substr(cast(a.activity_date_key as varchar), 1, 4), '-',
            substr(cast(a.activity_date_key as varchar), 5, 2)
        )                                                   as activity_year_and_month,
        1                                                   as is_active_day,
        0                                                   as new_enrollments,
        0                                                   as certificates_earned,
        a.videos_played,
        a.problems_attempted,
        a.chatbot_interactions
    from {{ source('dimensional', 'afact_learner_courserun_daily_activity') }} a
    join {{ source('dimensional', 'afact_learner_courserun_progress') }} p
        on a.user_fk = p.user_fk and a.courserun_fk = p.courserun_fk
    where a.platform = 'mitxonline'

    union all

    select
        user_fk,
        courserun_fk,
        substr(enrollment_created_on, 1, 7),
        0, 1, 0, 0, 0, 0
    from {{ source('dimensional', 'afact_learner_courserun_progress') }}
    where platform = 'mitxonline'
      and enrollment_is_active
      and enrollment_created_on is not null

    union all

    select
        user_fk,
        courserun_fk,
        substr(certificate_issued_on, 1, 7),
        0, 0, 1, 0, 0, 0
    from {{ source('dimensional', 'afact_learner_courserun_progress') }}
    where platform = 'mitxonline'
      and is_certified
{% endmacro %}

{# One row per (learner, course run) enrollment with its all-time activity totals. The
   counters sum the fact's per-day distinct counts, so a video played on two days counts
   twice, as in mv_b2b_learner_enrollment. #}
{% macro b2b_learner_courserun_engagement() %}
    select
        p.user_fk,
        p.courserun_fk,
        p.is_certified,
        coalesce(a.days_active, 0)                          as days_active,
        coalesce(a.videos_played, 0)                        as videos_played,
        coalesce(a.problems_attempted, 0)                   as problems_attempted,
        coalesce(a.chatbot_interactions, 0)                 as chatbot_interactions
    from {{ source('dimensional', 'afact_learner_courserun_progress') }} p
    left join (
        select
            user_fk,
            courserun_fk,
            count(distinct activity_date_key)               as days_active,
            sum(videos_played)                              as videos_played,
            sum(problems_attempted)                         as problems_attempted,
            sum(chatbot_interactions)                       as chatbot_interactions
        from {{ source('dimensional', 'afact_learner_courserun_daily_activity') }}
        where platform = 'mitxonline'
        group by user_fk, courserun_fk
    ) a
        on p.user_fk = a.user_fk and p.courserun_fk = a.courserun_fk
    where p.platform = 'mitxonline'
{% endmacro %}
