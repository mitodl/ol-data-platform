{{ config(
    materialized='incremental',
    unique_key=['courserun_pk', 'effective_date'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- EdxOrg course runs don't carry upgrade_deadline in their own source; the value is stored
-- in MicroMasters course run records keyed by courserun_edxorg_readable_id.
-- Filter to edxorg-platform rows only to prevent cross-platform false matches and
-- avoid 1:N fan-out if multiple MicroMasters rows share the same readable ID.
with micromasters_courseruns as (
    select courserun_edxorg_readable_id, courserun_upgrade_deadline
    from
        (
            select
                courserun_edxorg_readable_id
                , courserun_upgrade_deadline
                , row_number() over (
                    partition by courserun_edxorg_readable_id order by courserun_id desc
                ) as _row_num
            from {{ ref('stg__micromasters__app__postgres__courses_courserun') }}
            where courserun_platform = '{{ var("edxorg") }}'
        ) as mcr_deduped
    where _row_num = 1
)

-- MicroMasters stores exam run metadata (semester label and passing grade threshold)
-- keyed by examrun_readable_id, which matches the MITxOnline courserun_readable_id for
-- proctored exam course runs. These are the only platform-specific dimensional attributes
-- that originate outside the platform's own course run record.
, micromasters_examruns as (
    select examrun_readable_id, examrun_semester, examrun_passing_grade
    from (
        select
            examrun_readable_id
            , examrun_semester
            , examrun_passing_grade
            , row_number() over (
                partition by examrun_readable_id
                order by examrun_updated_on desc nulls last, examrun_id desc
            ) as _row_num
        from {{ ref('stg__micromasters__app__postgres__exams_examrun') }}
    ) as deduped_examruns
    where _row_num = 1
)

, mitxonline_courseruns as (
    select
        cr.courserun_readable_id
        , cr.courserun_id as source_id
        , cr.course_id
        , cr.courserun_title
        , cr.courserun_start_on
        , cr.courserun_end_on
        , cr.courserun_enrollment_start_on as enrollment_start
        , cr.courserun_enrollment_end_on as enrollment_end
        , cr.courserun_is_live
        , cr.courserun_created_on
        , cs.course_readable_id
        , er.examrun_semester as semester
        , er.examrun_passing_grade as passing_grade
        , 'mitxonline' as platform
        , cr.courserun_upgrade_deadline
    from {{ ref('int__mitxonline__course_runs') }} as cr
    left join micromasters_examruns as er
        on cr.courserun_readable_id = er.examrun_readable_id
    left join {{ ref('stg__mitxonline__app__postgres__courses_course') }} as cs
        on cr.course_id = cs.course_id
)

, mitxpro_courseruns as (
    select
        courserun_readable_id
        , courserun_id as source_id
        , course_id
        , courserun_title
        , courserun_start_on
        , courserun_end_on
        , courserun_enrollment_start_on as enrollment_start
        , courserun_enrollment_end_on as enrollment_end
        , courserun_is_live
        , courserun_created_on
        , cast(null as varchar) as course_readable_id
        , cast(null as varchar) as semester
        , cast(null as double) as passing_grade
        , 'mitxpro' as platform
        , cast(null as varchar) as courserun_upgrade_deadline
    from {{ ref('int__mitxpro__course_runs') }}
)

, edxorg_courseruns as (
    select
        cr.courserun_readable_id
        , cast(null as integer) as source_id
        , cast(null as integer) as course_id
        , cr.courserun_title
        , cr.courserun_start_date as courserun_start_on  -- edxorg uses _date suffix
        , cr.courserun_end_date as courserun_end_on
        , cr.courserun_enrollment_start_date as enrollment_start
        , cr.courserun_enrollment_end_date as enrollment_end
        , cr.courserun_is_published as courserun_is_live
        , cast(null as varchar) as courserun_created_on
        , cast(null as varchar) as course_readable_id
        , cast(null as varchar) as semester
        , cast(null as double) as passing_grade
        , 'edxorg' as platform
        , mc.courserun_upgrade_deadline
    from {{ ref('int__edxorg__mitx_courseruns') }} as cr
    left join micromasters_courseruns as mc on cr.courserun_readable_id = mc.courserun_edxorg_readable_id
)

, residential_courseruns as (
    select
        courserun_readable_id
        , cast(null as integer) as source_id
        , cast(null as integer) as course_id
        , courserun_title
        , courserun_start_on
        , courserun_end_on
        , courserun_enrollment_start_on as enrollment_start
        , courserun_enrollment_end_on as enrollment_end
        , cast(null as boolean) as courserun_is_live
        , courserun_created_on
        , cast(null as varchar) as course_readable_id
        , cast(null as varchar) as semester
        , cast(null as double) as passing_grade
        , 'residential' as platform
        , cast(null as varchar) as courserun_upgrade_deadline
    from {{ ref('int__mitxresidential__courseruns') }}
)

, bootcamps_courseruns as (
    select
        runs.courserun_readable_id
        , runs.courserun_id as source_id
        , runs.course_id
        , runs.courserun_title
        , runs.courserun_start_on
        , runs.courserun_end_on
        , cast(null as varchar) as enrollment_start
        , cast(null as varchar) as enrollment_end
        , false as courserun_is_live
        , cast(null as varchar) as courserun_created_on
        , runs.course_readable_id
        , cast(null as varchar) as semester
        , cast(null as double) as passing_grade
        , 'bootcamps' as platform
        , cast(null as varchar) as courserun_upgrade_deadline
    from {{ ref('int__bootcamps__course_runs') }} as runs
)

-- Emeritus and Global Alumni have no course run table; a run is identified by the Wrike run code
-- on each enrollment. Codes that match a MITxPro run are that run. The rest become runs of their
-- own, with attributes from the most recent enrollment row, matching int__combined__course_runs.
-- Their course_readable_id is the course code parsed from the run code, which links them to a
-- course in courseruns_with_fk.
, mitxpro_external_run_codes as (
    select distinct courserun_external_readable_id
    from {{ ref('int__mitxpro__course_runs') }}
    where courserun_external_readable_id is not null
        and courserun_external_readable_id != ''
)

, emeritus_courseruns as (
    select
        courserun_readable_id
        , cast(null as integer) as source_id
        , cast(null as integer) as course_id
        , courserun_title
        , courserun_start_on
        , courserun_end_on
        , cast(null as varchar) as enrollment_start
        , cast(null as varchar) as enrollment_end
        , cast(null as boolean) as courserun_is_live
        , cast(null as varchar) as courserun_created_on
        , {{ wrike_course_code('courserun_readable_id') }} as course_readable_id
        , cast(null as varchar) as semester
        , cast(null as double) as passing_grade
        , 'emeritus' as platform
        , cast(null as varchar) as courserun_upgrade_deadline
    from (
        select
            enrollments.courserun_external_readable_id as courserun_readable_id
            , enrollments.courserun_title
            , enrollments.courserun_start_on
            , enrollments.courserun_end_on
            , row_number() over (
                partition by enrollments.courserun_external_readable_id
                order by
                    enrollments.courserun_start_on desc
                    , enrollments.courserun_end_on desc
                    , enrollments.enrollment_created_on desc
                    , enrollments.courserun_title asc
            ) as row_num
        from {{ ref('stg__emeritus__api__bigquery__user_enrollments') }} as enrollments
        left join mitxpro_external_run_codes
            on enrollments.courserun_external_readable_id
            = mitxpro_external_run_codes.courserun_external_readable_id
        where
            enrollments.courserun_external_readable_id is not null
            and mitxpro_external_run_codes.courserun_external_readable_id is null
    ) as runs
    where row_num = 1
)

, global_alumni_courseruns as (
    select
        courserun_readable_id
        , cast(null as integer) as source_id
        , cast(null as integer) as course_id
        , courserun_title
        , courserun_start_on
        , courserun_end_on
        , cast(null as varchar) as enrollment_start
        , cast(null as varchar) as enrollment_end
        , cast(null as boolean) as courserun_is_live
        , cast(null as varchar) as courserun_created_on
        , {{ wrike_course_code('courserun_readable_id') }} as course_readable_id
        , cast(null as varchar) as semester
        , cast(null as double) as passing_grade
        , 'global_alumni' as platform
        , cast(null as varchar) as courserun_upgrade_deadline
    from (
        select
            enrollments.courserun_external_readable_id as courserun_readable_id
            , enrollments.courserun_title
            , enrollments.courserun_start_on
            , enrollments.courserun_end_on
            -- no enrollment_created_on in this source
            , row_number() over (
                partition by enrollments.courserun_external_readable_id
                order by
                    enrollments.courserun_start_on desc
                    , enrollments.courserun_end_on desc
                    , enrollments.user_gdpr_consent_date desc
                    , enrollments.courserun_title asc
            ) as row_num
        from {{ ref('stg__global_alumni__api__bigquery__user_enrollments') }} as enrollments
        left join mitxpro_external_run_codes
            on enrollments.courserun_external_readable_id
            = mitxpro_external_run_codes.courserun_external_readable_id
        where
            enrollments.courserun_external_readable_id is not null
            and mitxpro_external_run_codes.courserun_external_readable_id is null
    ) as runs
    where row_num = 1
)

, combined_courseruns as (
    select * from mitxonline_courseruns
    union all
    select * from mitxpro_courseruns
    union all
    select * from edxorg_courseruns
    union all
    select * from residential_courseruns
    union all
    select * from bootcamps_courseruns
    union all
    select * from emeritus_courseruns
    union all
    select * from global_alumni_courseruns
)

-- Pre-compute course_readable_id for all platforms so the dim_course join is a simple equality
, combined_courseruns_resolved as (
    select
        courserun_readable_id
        , source_id
        , course_id
        , courserun_title
        , courserun_start_on
        , courserun_end_on
        , enrollment_start
        , enrollment_end
        , courserun_is_live
        , courserun_created_on
        , platform
        , semester
        , passing_grade
        , coalesce(
            course_readable_id,
            case
                when courserun_readable_id like 'course-v1:%'
                    then substring(
                        courserun_readable_id,
                        1,
                        length(courserun_readable_id)
                        - strpos(reverse(courserun_readable_id), '+')
                    )
                when {{ regexp_like('courserun_readable_id', "'^[^/]+/[^/]+/[^/]+'") }}
                    then substring(
                        courserun_readable_id,
                        1,
                        length(courserun_readable_id)
                        - strpos(reverse(courserun_readable_id), '/')
                    )
                else courserun_readable_id
            end
        ) as course_readable_id
        , courserun_upgrade_deadline
    from combined_courseruns
)

-- Join to dim_course to get course_fk
, external_mitxpro_course_links as (
    {{ wrike_course_codes_of_external_mitxpro_courses() }}
)

, dim_course as (
    select
        course_pk
        , course_readable_id
        , primary_platform
    from {{ ref('dim_course') }}
    where is_current = true
)

, courseruns_with_fk as (
    select
        combined_courseruns_resolved.*
        , dim_course.course_pk as course_fk
    from combined_courseruns_resolved
    -- An Emeritus or Global Alumni run of an external xPro course belongs to that xPro course:
    -- the partner's own course for the code if there is one, else the other partner's
    left join external_mitxpro_course_links as same_partner_links
        on combined_courseruns_resolved.platform in ('emeritus', 'global_alumni')
        and combined_courseruns_resolved.course_readable_id = same_partner_links.wrike_course_code
    left join (
        select distinct wrike_course_code_without_partner, mitxpro_course_readable_id
        from external_mitxpro_course_links
    ) as other_partner_links
        on combined_courseruns_resolved.platform in ('emeritus', 'global_alumni')
        and same_partner_links.wrike_course_code is null
        and {{ wrike_course_code('combined_courseruns_resolved.courserun_readable_id', include_partner=false) }}
        = other_partner_links.wrike_course_code_without_partner
    left join dim_course
        on dim_course.primary_platform = case
            when coalesce(
                same_partner_links.mitxpro_course_readable_id, other_partner_links.mitxpro_course_readable_id
            ) is not null then 'mitxpro'
            else combined_courseruns_resolved.platform
        end
        and dim_course.course_readable_id = coalesce(
            same_partner_links.mitxpro_course_readable_id
            , other_partner_links.mitxpro_course_readable_id
            , combined_courseruns_resolved.course_readable_id
        )
)

, courseruns_with_all_fks as (
    select
        courseruns_with_fk.*
        , dim_platform_lookup.platform_pk as platform_fk
        -- Create date keys for date dimension joins (parse to timestamp then format as YYYYMMDD)
        , {{ iso8601_to_date_key('courserun_start_on') }} as courserun_start_date_key
        , {{ iso8601_to_date_key('courserun_end_on') }} as courserun_end_date_key
        , {{ iso8601_to_date_key('enrollment_start') }} as enrollment_start_date_key
        , {{ iso8601_to_date_key('enrollment_end') }} as enrollment_end_date_key
    from courseruns_with_fk
    left join (
        select platform_pk, platform_readable_id
        from {{ ref('dim_platform') }}
    ) as dim_platform_lookup
        on courseruns_with_fk.platform = dim_platform_lookup.platform_readable_id
)

-- DISTINCT here and in records_to_expire keeps the merge idempotent when upstream
-- briefly carries duplicate copies of a run. Without it each copy became its own current
-- row, and the expire join multiplied existing x incoming rows on every later change
-- (2026-09-01: two runs grew to ~1,000 versions in a day).
, final as (
    select distinct
        {{ dbt_utils.generate_surrogate_key([
            'platform',
            'courserun_readable_id'
        ]) }} as courserun_pk
        , courserun_readable_id
        , source_id
        , course_fk
        , platform_fk
        , platform
        , courserun_title
        , courserun_start_date_key
        , courserun_end_date_key
        , enrollment_start_date_key
        , enrollment_end_date_key
        , courserun_start_on
        , courserun_end_on
        , enrollment_start
        , enrollment_end
        , courserun_is_live
        , courserun_created_on
        , semester
        , passing_grade
        , current_timestamp as effective_date
        , cast(null as timestamp) as end_date
        , true as is_current
        , courserun_upgrade_deadline
    from courseruns_with_all_fks

    {% if is_incremental() %}
    where not exists (
        select 1
        from {{ this }} as existing
        where
            existing.courserun_readable_id = courseruns_with_all_fks.courserun_readable_id
            and existing.platform = courseruns_with_all_fks.platform
            and existing.is_current = true
            and coalesce(existing.courserun_title, '') = coalesce(courseruns_with_all_fks.courserun_title, '')
            and coalesce(existing.courserun_start_on, '') = coalesce(courseruns_with_all_fks.courserun_start_on, '')
            and coalesce(existing.courserun_end_on, '') = coalesce(courseruns_with_all_fks.courserun_end_on, '')
            and coalesce(existing.enrollment_start, '') = coalesce(courseruns_with_all_fks.enrollment_start, '')
            and coalesce(existing.enrollment_end, '') = coalesce(courseruns_with_all_fks.enrollment_end, '')
            and coalesce(existing.courserun_is_live, false) = coalesce(courseruns_with_all_fks.courserun_is_live, false)
            and coalesce(existing.courserun_upgrade_deadline, '')
            = coalesce(courseruns_with_all_fks.courserun_upgrade_deadline, '')
            and coalesce(existing.semester, '') = coalesce(courseruns_with_all_fks.semester, '')
            and coalesce(existing.passing_grade, -1.0) = coalesce(courseruns_with_all_fks.passing_grade, -1.0)
    )
    {% endif %}
)

{% if is_incremental() %}
-- Expire prior current rows that have changed
, records_to_expire as (
    select distinct
        existing.courserun_pk
        , existing.courserun_readable_id
        , existing.source_id
        , existing.course_fk
        , existing.platform_fk
        , existing.platform
        , existing.courserun_title
        , existing.courserun_start_date_key
        , existing.courserun_end_date_key
        , existing.enrollment_start_date_key
        , existing.enrollment_end_date_key
        , existing.courserun_start_on
        , existing.courserun_end_on
        , existing.enrollment_start
        , existing.enrollment_end
        , existing.courserun_is_live
        , existing.courserun_created_on
        , existing.semester
        , existing.passing_grade
        , existing.effective_date
        , current_timestamp as end_date
        , false as is_current
        , existing.courserun_upgrade_deadline
    from {{ this }} as existing
    inner join final as new_records
        on existing.courserun_readable_id = new_records.courserun_readable_id
        and existing.platform = new_records.platform
    where existing.is_current = true
)

, combined as (
    select * from final
    union all
    select * from records_to_expire
)

select * from combined
{% else %}
select * from final
{% endif %}
