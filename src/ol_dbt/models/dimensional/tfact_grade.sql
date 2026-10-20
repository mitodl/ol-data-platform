{{ config(
    materialized='incremental',
    unique_key='grade_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Consolidate course run grades from all platforms. The is_passing rule differs per
-- platform; the model YAML states each one.
-- edX.org rows include MicroMasters-linked grades (micromasters_program_id IS NOT NULL).
-- The MicroMasters DEDP combined grade (int__micromasters__course_grades) is a course grade
-- that includes the proctored exam, so it has no row here.
with mitxonline_grades as (
    select
        cast(courserungrade_id as varchar) as grade_id
        , user_id
        , courserun_readable_id
        , courserungrade_grade as grade_value
        , courserungrade_letter_grade as letter_grade
        , courserungrade_is_passing as is_passing
        , courserungrade_created_on as grade_created_on
        , courserungrade_updated_on as grade_updated_on
        , 'mitxonline' as platform
    from {{ ref('int__mitxonline__courserun_grades') }}
    -- Filter to MITx Online-hosted runs only; edX.org-hosted runs are captured via edxorg_grades
    where courserun_platform = '{{ var("mitxonline") }}'
)

, mitxpro_grades as (
    select
        cast(courserungrade_id as varchar) as grade_id
        , user_id
        , courserun_readable_id
        , courserungrade_grade as grade_value
        , courserungrade_letter_grade as letter_grade
        , courserungrade_is_passing as is_passing
        , courserungrade_created_on as grade_created_on
        , courserungrade_updated_on as grade_updated_on
        , 'mitxpro' as platform
    from {{ ref('int__mitxpro__courserun_grades') }}
)

, edxorg_grades as (
    select
        -- edxorg has no native grade PK; construct stable surrogate from natural key
        {{ dbt_utils.generate_surrogate_key(['cast(user_id as varchar)', 'courserun_readable_id']) }} as grade_id
        , user_id
        , courserun_readable_id
        , courserungrade_user_grade as grade_value  -- column name differs from other platforms
        , cast(null as varchar) as letter_grade      -- edxorg does not expose letter grades
        , courserungrade_is_passing as is_passing
        , cast(null as varchar) as grade_created_on  -- edxorg source has no grade timestamp
        , cast(null as varchar) as grade_updated_on
        , 'edxorg' as platform
    from {{ ref('int__edxorg__mitx_courserun_grades') }}
)

-- The raw table keeps grades that Open edX deleted, so a learner can have two for one
-- run. Keep the newest, the one Open edX still has.
, residential_grades_ranked as (
    select
        *
        , row_number() over (
            partition by user_id, courserun_readable_id
            order by courserungrade_created_on desc, courserungrade_id desc
        ) as grade_rank
    from {{ ref('int__mitxresidential__courserun_grades') }}
)

, residential_grades as (
    select
        -- Natural key, as for edxorg: a replacement grade then keeps the grade_key, so
        -- an incremental run overwrites the old row instead of adding a second one
        {{ dbt_utils.generate_surrogate_key(['cast(user_id as varchar)', 'courserun_readable_id']) }} as grade_id
        , user_id
        , courserun_readable_id
        , courserungrade_grade as grade_value
        , courserungrade_letter_grade as letter_grade
        -- residential has no passing flag; a non-empty letter grade means passed, as in
        -- int__combined__courserun_enrollments
        , coalesce(courserungrade_letter_grade != '', false) as is_passing
        , courserungrade_created_on as grade_created_on
        , courserungrade_updated_on as grade_updated_on
        , 'residential' as platform
    from residential_grades_ranked
    where grade_rank = 1
)

, combined_grades as (
    select * from mitxonline_grades
    union all
    select * from mitxpro_grades
    union all
    select * from edxorg_grades
    union all
    select * from residential_grades
)

, user_lookup as (
    select
        user_pk
        , mitxonline_application_user_id
        , mitxpro_application_user_id
        , edxorg_openedx_user_id
        , residential_openedx_user_id
    from {{ ref('dim_user') }}
    where user_pk is not null
)

, dim_course_run as (
    select courserun_pk, courserun_readable_id, platform
    from {{ ref('dim_course_run') }}
    where is_current = true
)

, dim_platform_lookup as (
    select platform_pk, platform_readable_id
    from {{ ref('dim_platform') }}
)

, grades_with_fks as (
    select
        combined_grades.*
        , coalesce(
            case when combined_grades.platform = 'mitxonline'
                then ul_mitxonline.user_pk
            end,
            case when combined_grades.platform = 'mitxpro'
                then ul_mitxpro.user_pk
            end,
            case when combined_grades.platform = 'edxorg'
                then ul_edxorg.user_pk
            end,
            case when combined_grades.platform = 'residential'
                then ul_residential.user_pk
            end
        ) as user_fk
        , dim_course_run.courserun_pk as courserun_fk
        , dim_platform_lookup.platform_pk as platform_fk
        , {{ iso8601_to_date_key('grade_created_on') }} as grade_date_key
    from combined_grades
    left join user_lookup as ul_mitxonline
        on combined_grades.platform = 'mitxonline'
        and combined_grades.user_id = ul_mitxonline.mitxonline_application_user_id
    left join user_lookup as ul_mitxpro
        on combined_grades.platform = 'mitxpro'
        and combined_grades.user_id = ul_mitxpro.mitxpro_application_user_id
    left join user_lookup as ul_edxorg
        on combined_grades.platform = 'edxorg'
        and combined_grades.user_id = ul_edxorg.edxorg_openedx_user_id
    left join user_lookup as ul_residential
        on combined_grades.platform = 'residential'
        and combined_grades.user_id = ul_residential.residential_openedx_user_id
    left join dim_course_run
        on combined_grades.courserun_readable_id = dim_course_run.courserun_readable_id
        and combined_grades.platform = dim_course_run.platform
    left join dim_platform_lookup
        on combined_grades.platform = dim_platform_lookup.platform_readable_id
)

{% if is_incremental() %}
-- Pre-compute per-platform watermarks in a single scan of {{ this }}.
-- Using a pre-computed CTE + equijoin avoids the correlated subquery fan-out anti-pattern.
-- edxorg grades have null timestamps; the `w.max_activity_on is null` branch in the WHERE
-- clause covers new platforms, and the `gwf.grade_created_on is null` branch re-ingests
-- all edxorg rows on every incremental run (acceptable: edxorg source is static snapshots).
, incremental_watermarks as (
    select
        platform as watermark_platform
        , max(coalesce(grade_updated_on, grade_created_on)) as max_activity_on
    from {{ this }}
    group by platform
)

-- Snapshot of the target's current (grade_key, user_fk) pairs, used to re-select rows
-- whose user_fk has gone stale after a dim_user re-key (the source row itself did not
-- change, so the activity-timestamp watermark alone would never catch it). One row per
-- key: a key a past run wrote twice would otherwise match each source row twice and be
-- written twice again. It is re-selected instead, so delete+insert leaves one copy.
, stale_user_fk_lookup as (
    select
        grade_key
        , max(user_fk) as stored_user_fk
        , count(*) > 1 as stored_has_copies
    from {{ this }}
    group by grade_key
)
{% endif %}

, final as (
    select
        {{ dbt_utils.generate_surrogate_key(['grade_id', 'platform']) }} as grade_key
        , gwf.grade_id
        , gwf.grade_date_key
        , gwf.user_fk
        , gwf.courserun_fk
        , gwf.platform_fk
        , gwf.platform
        , gwf.grade_value
        , gwf.is_passing
        , gwf.letter_grade
        , gwf.grade_created_on
        , gwf.grade_updated_on
    from grades_with_fks as gwf

    {% if is_incremental() %}
    -- Left join preserves grades from platforms not yet in the target table
    left join incremental_watermarks w
        on w.watermark_platform = gwf.platform
    left join stale_user_fk_lookup as sufk
        on sufk.grade_key = {{ dbt_utils.generate_surrogate_key(["gwf.grade_id", "gwf.platform"]) }}
    where (
        w.max_activity_on is null  -- platform not yet in target, include all
        -- Use >= to capture updated_on changes within the same second as the watermark
        or coalesce(gwf.grade_updated_on, gwf.grade_created_on) >= w.max_activity_on
        -- edxorg has no timestamps; always re-ingest (delete+insert is idempotent)
        or gwf.grade_created_on is null
        -- dim_user re-key: re-select rows whose resolved user_fk no longer matches the target
        or sufk.stored_user_fk is distinct from gwf.user_fk
        or sufk.stored_has_copies
    )
    {% endif %}
)

-- No dedup here: each lookup above is unique on its join key, so a duplicate grade_key
-- means a source or a lookup lost its grain. The error-severity unique test on grade_key
-- reports it. A row_number here would keep one copy's FKs at random.
select
    grade_key
    , grade_id
    , grade_date_key
    , user_fk
    , courserun_fk
    , platform_fk
    , platform
    , grade_value
    , is_passing
    , letter_grade
    , grade_created_on
    , grade_updated_on
from final
