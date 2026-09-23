{{ config(
    materialized='materialized_view',
    distributed_by=['organization_key'],
    buckets=8,
    refresh_method='manual',
) }}

-- Grain: org x contract x course_run x learner. Refreshed by the Dagster b2b_organization
-- MV-refresh asset. Individually identifying: read by ol-analytics-api's
-- b2b_learner_records tenant, never by the k-anonymized b2b_dashboard tenant.
-- The organization is reached through the contract that owns the course run, never
-- organization_administration_report's free-text organization_key fallback, which
-- would show a partner learners who aren't theirs.
-- Not filtered to the organization's current roster: a learner removed from the
-- organization keeps their enrollment in its contract runs. Roster history is kept in
-- snapshot_mitxonline_b2b_userorganization.
-- Activity is pre-aggregated to the (user, course run) join key so it cannot fan out.
-- activity_date_key is YYYYMMDD, so its max is the latest day; it is read as a date
-- through dim_date. The counters sum the fact's per-day distinct counts, so a video
-- played on two days counts twice, as in b2b_analytics' total_videos_watched.
-- Activity does not move record_updated_on; see the column's description.
with activity as (
    select
        user_fk,
        courserun_fk,
        max(activity_date_key)                                                          as last_active_date_key,
        count(distinct activity_date_key)                                               as days_active,
        sum(videos_played)                                                              as videos_played,
        sum(problems_attempted)                                                         as problems_attempted,
        sum(chatbot_interactions)                                                       as chatbot_interactions
    from {{ source('dimensional', 'afact_learner_courserun_daily_activity') }}
    where platform = 'mitxonline'
    group by user_fk, courserun_fk
)

select
    org.organization_key,
    org.sso_organization_id,
    org.organization_name,
    c.contract_pk,
    c.contract_id,
    c.b2b_contract_name,
    cr.courserun_pk,
    cr.courserun_readable_id,
    cr.courserun_title,
    cr.courserun_start_on,
    cr.courserun_end_on,
    u.user_pk,
    u.user_global_id,
    u.email,
    u.full_name,
    e.enrollment_created_on,
    e.enrollment_is_active,
    e.enrollment_mode,
    e.enrollment_status,
    g.is_passing,
    g.grade_value,
    g.letter_grade,
    cert.certificate_issued_on,
    cert.certificate_is_revoked,
    cast(d.date as date)                                                                as last_active_on,
    coalesce(a.days_active, 0)                                                          as days_active,
    coalesce(a.videos_played, 0)                                                        as videos_played,
    coalesce(a.problems_attempted, 0)                                                   as problems_attempted,
    coalesce(a.chatbot_interactions, 0)                                                 as chatbot_interactions,
    -- Every mitxonline timestamp here is an ISO-8601 string from the same macro, so
    -- greatest() compares them correctly. StarRocks' greatest() returns null if any
    -- argument is null, so each is coalesced to '', which sorts below any real date.
    nullif(greatest(
        coalesce(e.enrollment_updated_on, ''),
        coalesce(e.enrollment_created_on, ''),
        coalesce(g.grade_updated_on, ''),
        coalesce(cert.certificate_updated_on, '')
    ), '')                                                                              as record_updated_on
from {{ source('dimensional', 'bridge_organization_courserun') }} boc
join {{ source('dimensional', 'dim_contract') }} c
    on boc.contract_fk = c.contract_pk
join {{ source('dimensional', 'dim_organization') }} org
    on c.organization_fk = org.organization_pk
join {{ source('dimensional', 'dim_course_run') }} cr
    on boc.courserun_fk = cr.courserun_pk
join {{ source('dimensional', 'tfact_enrollment') }} e
    on boc.courserun_fk = e.courserun_fk
join {{ source('dimensional', 'dim_user') }} u
    on e.user_fk = u.user_pk
left join {{ source('dimensional', 'tfact_grade') }} g
    on e.user_fk = g.user_fk and e.courserun_fk = g.courserun_fk
left join {{ source('dimensional', 'tfact_certificate') }} cert
    on e.user_fk = cert.user_fk and e.courserun_fk = cert.courserun_fk
left join activity a
    on e.user_fk = a.user_fk and e.courserun_fk = a.courserun_fk
left join {{ source('dimensional', 'dim_date') }} d
    on a.last_active_date_key = d.date_key
where org.platform = 'mitxonline'
  and cr.is_current = true
  -- The API's learner_id is required. Filtered here rather than asserted by a dbt
  -- test, which would run against the previous refresh and block every MV's refresh.
  and u.user_global_id is not null
