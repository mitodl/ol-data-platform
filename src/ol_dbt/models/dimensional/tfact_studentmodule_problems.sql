{{ config(
    materialized='incremental',
    unique_key=['platform', 'studentmodulehistoryextended_id'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns',
    properties={
        "partitioning": "ARRAY['platform']",
    }
) }}

-- Precompute incremental watermarks once (1 scan instead of 6)
-- Each macro call receives the pre-fetched value for its platform.
{% if is_incremental() %}
with watermarks as (
    select platform, max(event_timestamp) as max_ts
    from {{ this }}
    group by platform
)

, mitxonline_watermark as (select max_ts from watermarks where platform = 'mitxonline')
, mitxpro_watermark as (select max_ts from watermarks where platform = 'mitxpro')
, residential_watermark as (select max_ts from watermarks where platform = 'residential')

, mitxonline_studentmodule_problems as (
    {{ generate_studentmodule_problem_events(
        ref('stg__mitxonline__openedx__mysql__courseware_studentmodule'),
        ref('stg__mitxonline__openedx__courseware_studentmodulehistoryextended'),
        'openedx_user_id',
        'mitxonline',
        watermark_expr='(select max_ts from mitxonline_watermark)'
    ) }}
)

, mitxpro_studentmodule_problems as (
    {{ generate_studentmodule_problem_events(
        ref('stg__mitxpro__openedx__mysql__courseware_studentmodule'),
        ref('stg__mitxpro__openedx__courseware_studentmodulehistoryextended'),
        'openedx_user_id',
        'mitxpro',
        watermark_expr='(select max_ts from mitxpro_watermark)'
    ) }}
)

, mitxresidential_studentmodule_problems as (
    {{ generate_studentmodule_problem_events(
        ref('stg__mitxresidential__openedx__courseware_studentmodule'),
        ref('stg__mitxresidential__openedx__courseware_studentmodulehistoryextended'),
        'user_id',
        'residential',
        watermark_expr='(select max_ts from residential_watermark)'
    ) }}
)
{% else %}
with mitxonline_studentmodule_problems as (
    {{ generate_studentmodule_problem_events(
        ref('stg__mitxonline__openedx__mysql__courseware_studentmodule'),
        ref('stg__mitxonline__openedx__courseware_studentmodulehistoryextended'),
        'openedx_user_id',
        'mitxonline'
    ) }}
)

, mitxpro_studentmodule_problems as (
    {{ generate_studentmodule_problem_events(
        ref('stg__mitxpro__openedx__mysql__courseware_studentmodule'),
        ref('stg__mitxpro__openedx__courseware_studentmodulehistoryextended'),
        'openedx_user_id',
        'mitxpro'
    ) }}
)

, mitxresidential_studentmodule_problems as (
    {{ generate_studentmodule_problem_events(
        ref('stg__mitxresidential__openedx__courseware_studentmodule'),
        ref('stg__mitxresidential__openedx__courseware_studentmodulehistoryextended'),
        'user_id',
        'residential'
    ) }}
)
{% endif %}

, users as (
    select
        user_pk
        , mitxonline_openedx_user_id
        , mitxpro_openedx_user_id
        , residential_openedx_user_id
        , user_mitxonline_username
        , user_mitxpro_username
        , user_residential_username
    from {{ ref('dim_user') }}
)

-- Per-platform deduped lookups prevent fan-out when the same openedx_user_id
-- appears in multiple dim_user rows (merged identities).
, mitxonline_users as (
    select min(user_pk) as user_pk, mitxonline_openedx_user_id, arbitrary(user_mitxonline_username) as user_username
    from users
    where mitxonline_openedx_user_id is not null
    group by mitxonline_openedx_user_id
)

, mitxpro_users as (
    select min(user_pk) as user_pk, mitxpro_openedx_user_id, arbitrary(user_mitxpro_username) as user_username
    from users
    where mitxpro_openedx_user_id is not null
    group by mitxpro_openedx_user_id
)

, residential_users as (
    select min(user_pk) as user_pk, residential_openedx_user_id, arbitrary(user_residential_username) as user_username
    from users
    where residential_openedx_user_id is not null
    group by residential_openedx_user_id
)

, mitxonline_studentmodule_combined as (
    select
        users.user_pk as user_fk
        , 'mitxonline' as platform
        , sm.user_id as openedx_user_id
        , users.user_username as user_username
        , sm.courserun_readable_id
        , sm.studentmodule_id
        , sm.studentmodulehistoryextended_id
        , sm.coursestructure_block_id as problem_block_id
        , sm.attempt
        , sm.seed
        , sm.correct_map
        , sm.answers
        , sm.grade
        , sm.max_grade
        , sm.success
        , {{ cast_timestamp_to_iso8601('sm.event_timestamp') }} as event_timestamp_iso8601
        , sm.event_timestamp
        , {{ timestamp_to_time_key('sm.event_timestamp') }} as time_fk
        , {{ iso8601_to_date_key(cast_timestamp_to_iso8601('sm.event_timestamp')) }} as date_fk
    from mitxonline_studentmodule_problems as sm
    left join mitxonline_users as users on sm.user_id = users.mitxonline_openedx_user_id
)

, mitxpro_studentmodule_combined as (
    select
        users.user_pk as user_fk
        , 'mitxpro' as platform
        , sm.user_id as openedx_user_id
        , users.user_username as user_username
        , sm.courserun_readable_id
        , sm.studentmodule_id
        , sm.studentmodulehistoryextended_id
        , sm.coursestructure_block_id as problem_block_id
        , sm.attempt
        , sm.seed
        , sm.correct_map
        , sm.answers
        , sm.grade
        , sm.max_grade
        , sm.success
        , {{ cast_timestamp_to_iso8601('sm.event_timestamp') }} as event_timestamp_iso8601
        , sm.event_timestamp
        , {{ timestamp_to_time_key('sm.event_timestamp') }} as time_fk
        , {{ iso8601_to_date_key(cast_timestamp_to_iso8601('sm.event_timestamp')) }} as date_fk
    from mitxpro_studentmodule_problems as sm
    left join mitxpro_users as users on sm.user_id = users.mitxpro_openedx_user_id
)

, residential_studentmodule_combined as (
    select
        users.user_pk as user_fk
        , 'residential' as platform
        , sm.user_id as openedx_user_id
        , users.user_username as user_username
        , sm.courserun_readable_id
        , sm.studentmodule_id
        , sm.studentmodulehistoryextended_id
        , sm.coursestructure_block_id as problem_block_id
        , sm.attempt
        , sm.seed
        , sm.correct_map
        , sm.answers
        , sm.grade
        , sm.max_grade
        , sm.success
        , {{ cast_timestamp_to_iso8601('sm.event_timestamp') }} as event_timestamp_iso8601
        , sm.event_timestamp
        , {{ timestamp_to_time_key('sm.event_timestamp') }} as time_fk
        , {{ iso8601_to_date_key(cast_timestamp_to_iso8601('sm.event_timestamp')) }} as date_fk
    from mitxresidential_studentmodule_problems as sm
    left join residential_users as users on sm.user_id = users.residential_openedx_user_id
)

, combined as (
    select *
    from mitxonline_studentmodule_combined

    union all

    select *
    from mitxpro_studentmodule_combined

    union all

    select *
    from residential_studentmodule_combined
)

select
    user_fk
    , platform
    , openedx_user_id
    , user_username
    , courserun_readable_id
    , studentmodule_id
    , studentmodulehistoryextended_id
    , problem_block_id
    , attempt
    , seed
    , correct_map
    , answers
    , grade
    , max_grade
    , success
    , event_timestamp_iso8601
    , event_timestamp
    , time_fk
    , date_fk
from combined
