{{ config(
    materialized='incremental',
    unique_key='grade_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Consolidate course run grades from all platforms
-- Note: edX.org rows include MicroMasters-linked grades (micromasters_program_id IS NOT NULL).
-- These are intentionally included here as edxorg grade records; MicroMasters as a distinct
-- platform source may be added in a future phase.
with mitxonline_grades as (
    select
        cast(courserungrade_id as varchar) as grade_id
        , user_id
        , courserun_id
        , cast(null as varchar) as courserun_readable_id  -- edxorg join key only
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
        , courserun_id
        , cast(null as varchar) as courserun_readable_id  -- edxorg join key only
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
        , cast(null as integer) as courserun_id  -- edxorg has no integer source_id
        , courserun_readable_id
        , courserungrade_user_grade as grade_value  -- column name differs from other platforms
        , cast(null as varchar) as letter_grade      -- edxorg does not expose letter grades
        , courserungrade_is_passing as is_passing
        , cast(null as varchar) as grade_created_on  -- edxorg source has no grade timestamp
        , cast(null as varchar) as grade_updated_on
        , 'edxorg' as platform
    from {{ ref('int__edxorg__mitx_courserun_grades') }}
)

, combined_grades as (
    select * from mitxonline_grades
    union all
    select * from mitxpro_grades
    union all
    select * from edxorg_grades
)

, user_lookup as (
    select
        user_pk
        , mitxonline_application_user_id
        , mitxpro_application_user_id
        , edxorg_openedx_user_id
    from {{ ref('dim_user') }}
    where user_pk is not null
)

, dim_course_run as (
    select courserun_pk, source_id, courserun_readable_id, platform
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
    left join dim_course_run
        on (
            -- mitxonline/mitxpro: join on integer source_id + platform
            (combined_grades.platform in ('mitxonline', 'mitxpro')
                and combined_grades.courserun_id = dim_course_run.source_id
                and combined_grades.platform = dim_course_run.platform)
            -- edxorg: join on readable_id + platform to prevent fan-out across platforms
            or (combined_grades.platform = 'edxorg'
                and combined_grades.courserun_readable_id = dim_course_run.courserun_readable_id
                and dim_course_run.platform = 'edxorg')
        )
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
{% endif %}

, final as (
    select
        {{ dbt_utils.generate_surrogate_key(['grade_id', 'platform']) }} as grade_key
        , grade_id
        , grade_date_key
        , user_fk
        , courserun_fk
        , platform_fk
        , gwf.platform
        , grade_value
        , is_passing
        , letter_grade
        , grade_created_on
        , grade_updated_on
    from grades_with_fks as gwf

    {% if is_incremental() %}
    -- Left join preserves grades from platforms not yet in the target table
    left join incremental_watermarks w
        on w.watermark_platform = gwf.platform
    where (
        w.max_activity_on is null  -- platform not yet in target, include all
        -- Use >= to capture updated_on changes within the same second as the watermark
        or coalesce(gwf.grade_updated_on, gwf.grade_created_on) >= w.max_activity_on
        -- edxorg has no timestamps; always re-ingest (delete+insert is idempotent)
        or gwf.grade_created_on is null
    )
    {% endif %}
)

-- Defensive dedup: UNION ALL across platform CTEs has no upstream uniqueness guarantee.
-- If any intermediate develops grain drift this guard prevents duplicate grade_key values
-- from corrupting incremental MERGE operations.
-- Note: QUALIFY is not supported by Trino; using ROW_NUMBER subquery instead.
, final_deduped as (
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
        , row_number() over (
            partition by grade_key
            order by coalesce(grade_updated_on, grade_created_on) desc nulls last
        ) as _row_num
    from final
)

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
from final_deduped
where _row_num = 1
