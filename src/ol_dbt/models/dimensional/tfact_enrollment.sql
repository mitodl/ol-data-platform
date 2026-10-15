{{ config(
    materialized='incremental',
    unique_key='enrollment_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

{#- The first incremental run after contract_fk is added reads a target without the column.
    In a unit test `this` is the fixture's name, a string, which always has it. -#}
{%- set target_has_contract_fk = false -%}
{%- if is_incremental() and this is string -%}
    {%- set target_has_contract_fk = true -%}
{%- elif is_incremental() -%}
    {%- set target_columns = adapter.get_columns_in_relation(this) | map(attribute='name') | map('lower') | list -%}
    {%- set target_has_contract_fk = 'contract_fk' in target_columns -%}
{%- endif %}

-- Consolidate enrollments from all platforms
with mitxonline_enrollments as (
    select
        cast(courserunenrollment_id as varchar) as enrollment_id
        , user_id
        , cast(null as varchar) as external_user_id
        , cast(null as varchar) as user_email
        , courserun_id
        , courserun_readable_id
        , null as program_id
        , 'course' as enrollment_scope
        , courserunenrollment_created_on as enrollment_created_on
        , courserunenrollment_updated_on as enrollment_updated_on
        , courserunenrollment_is_active as enrollment_is_active
        , courserunenrollment_enrollment_mode as enrollment_mode
        , courserunenrollment_enrollment_status as enrollment_status
        , 'mitxonline' as platform
        , 'mitxonline' as platform_code
        , courserunenrollment_is_edx_enrolled as enrollment_is_edx_enrolled
        -- mitxonline has no order FK on the enrollment record; order attribution for
        -- this platform relies on product-matching in downstream consumers.
        , cast(null as bigint) as order_id
        , b2b_contract_id
    from {{ ref('int__mitxonline__courserunenrollments') }}
)

-- A program-purchase order creates one ProgramEnrollment plus one CourseRunEnrollment per
-- course in the program, all sharing the same ecommerce_order_id. Use that shared order to
-- recover the program_id for course-run enrollments that came from a program order.
, mitxpro_program_enrollments_by_order as (
    select
        ecommerce_order_id
        , program_id
        , row_number() over (
            partition by ecommerce_order_id
            order by programenrollment_created_on desc, programenrollment_id desc
        ) as row_num
    from {{ ref('int__mitxpro__programenrollments') }}
    where ecommerce_order_id is not null
)

, mitxpro_enrollments as (
    select
        cast(enrollments.courserunenrollment_id as varchar) as enrollment_id
        , enrollments.user_id
        , cast(null as varchar) as external_user_id
        , cast(null as varchar) as user_email
        , enrollments.courserun_id
        , enrollments.courserun_readable_id
        , mitxpro_program_enrollments_by_order.program_id
        , 'course' as enrollment_scope
        , enrollments.courserunenrollment_created_on as enrollment_created_on
        , enrollments.courserunenrollment_updated_on as enrollment_updated_on
        , enrollments.courserunenrollment_is_active as enrollment_is_active
        , enrollments.courserunenrollment_enrollment_mode as enrollment_mode
        , enrollments.courserunenrollment_enrollment_status as enrollment_status
        , 'mitxpro' as platform
        , 'mitxpro' as platform_code
        , enrollments.courserunenrollment_is_edx_enrolled as enrollment_is_edx_enrolled
        -- xPRO is the only platform that stores an order FK directly on the enrollment
        -- record. This is authoritative regardless of subsequent deferral to a different
        -- course run or bundling into a program purchase — downstream consumers should
        -- prefer this over reconstructing order attribution via product-matching.
        , cast(enrollments.ecommerce_order_id as bigint) as order_id
        , cast(null as bigint) as b2b_contract_id
    from {{ ref('int__mitxpro__courserunenrollments') }} as enrollments
    left join mitxpro_program_enrollments_by_order
        on
            enrollments.ecommerce_order_id = mitxpro_program_enrollments_by_order.ecommerce_order_id
            and mitxpro_program_enrollments_by_order.row_num = 1
)

, edxorg_enrollments as (
    select
        -- Stable surrogate key: edxorg has no enrollment_id; use natural key (user + course run)
        {{ dbt_utils.generate_surrogate_key(['cast(user_id as varchar)', 'courserun_readable_id']) }} as enrollment_id
        , user_id
        , cast(null as varchar) as external_user_id
        , cast(null as varchar) as user_email
        , null as courserun_id  -- edxorg has no integer source_id
        , courserun_readable_id
        , null as program_id
        , 'course' as enrollment_scope
        , courserunenrollment_created_on as enrollment_created_on
        , cast(null as varchar) as enrollment_updated_on
        , courserunenrollment_is_active as enrollment_is_active
        , courserunenrollment_enrollment_mode as enrollment_mode
        , null as enrollment_status
        , 'edxorg' as platform
        , 'edxorg' as platform_code
        , true as enrollment_is_edx_enrolled
        , cast(null as bigint) as order_id
        , cast(null as bigint) as b2b_contract_id
    from {{ ref('int__edxorg__mitx_courserun_enrollments') }}
)

-- NOTE: This CTE captures MITx Online program enrollments only. MicroMasters program
-- enrollments on edX.org (tracked in int__edxorg__mitx_program_enrollments) are not
-- included here because edxorg MicroMasters programs use a separate program ID namespace
-- that does not match the program IDs in dim_program. Linking those enrollments requires
-- a separate mapping effort and is tracked as future work.
, program_enrollments as (
    select
        cast(programenrollment_id as varchar) as enrollment_id
        , user_id
        , cast(null as varchar) as external_user_id
        , cast(null as varchar) as user_email
        , null as courserun_id
        , cast(null as varchar) as courserun_readable_id
        , program_id
        , 'program' as enrollment_scope
        , programenrollment_created_on as enrollment_created_on
        , programenrollment_updated_on as enrollment_updated_on
        , programenrollment_is_active as enrollment_is_active
        , programenrollment_enrollment_mode as enrollment_mode
        , programenrollment_enrollment_status as enrollment_status
        , 'mitxonline' as platform
        , 'mitxonline' as platform_code
        , cast(null as boolean) as enrollment_is_edx_enrolled
        , cast(null as bigint) as order_id
        , b2b_contract_id
    from {{ ref('int__mitxonline__programenrollments') }}
)

, residential_enrollments as (
    select
        cast(courserunenrollment_id as varchar) as enrollment_id
        , user_id
        , cast(null as varchar) as external_user_id
        , cast(null as varchar) as user_email
        , null as courserun_id
        , courserun_readable_id
        , null as program_id
        , 'course' as enrollment_scope
        , courserunenrollment_created_on as enrollment_created_on
        , cast(null as varchar) as enrollment_updated_on
        , courserunenrollment_is_active as enrollment_is_active
        , courserunenrollment_enrollment_mode as enrollment_mode
        , null as enrollment_status
        , 'residential' as platform
        , 'residential' as platform_code
        , true as enrollment_is_edx_enrolled
        , cast(null as bigint) as order_id
        , cast(null as bigint) as b2b_contract_id
    from {{ ref('int__mitxresidential__courserun_enrollments') }}
)

, bootcamps_enrollments as (
    select
        cast(courserunenrollment_id as varchar) as enrollment_id
        , user_id
        , cast(null as varchar) as external_user_id
        , cast(null as varchar) as user_email
        , courserun_id
        , courserun_readable_id
        , null as program_id
        , 'course' as enrollment_scope
        , courserunenrollment_created_on as enrollment_created_on
        , courserunenrollment_updated_on as enrollment_updated_on
        , courserunenrollment_is_active as enrollment_is_active
        , cast(null as varchar) as enrollment_mode
        , courserunenrollment_enrollment_status as enrollment_status
        , 'bootcamps' as platform
        , 'bootcamps' as platform_code
        , cast(null as boolean) as enrollment_is_edx_enrolled
        , cast(null as bigint) as order_id
        , cast(null as bigint) as b2b_contract_id
    from {{ ref('int__bootcamps__courserunenrollments') }}
)

-- Emeritus/Global Alumni enrollments carry an external run code. Where an xPro run has the
-- same code (courserun_external_readable_id, as in int__combined__courserun_enrollments),
-- this lookup resolves it to that run's courserun_readable_id. Codes with no xPro run keep
-- the external code, which matches the platform's own dim_course_run row.
-- xPro can give two live runs one code (seven pairs since 2026-10-14, e.g. MCPO and MO_MCPO
-- for MO-MCPO-26-12#1). The newest run takes the code, so an enrollment resolves to one run.
, mitxpro_external_readable_id_lookup as (
    select courserun_external_readable_id, courserun_readable_id
    from (
        select
            courserun_external_readable_id
            , courserun_readable_id
            , row_number() over (
                partition by courserun_external_readable_id
                order by courserun_id desc
            ) as _row_num
        from {{ ref('int__mitxpro__course_runs') }}
        -- xPro runs with no external id carry '' rather than null; without this, a source row
        -- with an empty run code would fan out across all of them
        where courserun_external_readable_id is not null
            and courserun_external_readable_id != ''
    ) as ranked
    where _row_num = 1
)

, emeritus_enrollments as (
    select
        -- Source has no native enrollment_id. Keyed on the staging model's unique grain, not
        -- the student id: any change to this id moving forward would change the key and
        -- result in a duplicate record. The name is keyed as first and last, not the
        -- concatenated user_full_name, which can join two different splits of one name.
        {{ dbt_utils.generate_surrogate_key([
            'emeritus_enrollments.courserun_external_readable_id',
            'emeritus_enrollments.user_email',
            'emeritus_enrollments.user_first_name',
            'emeritus_enrollments.user_last_name'
        ]) }} as enrollment_id
        , cast(null as bigint) as user_id
        , emeritus_enrollments.user_id as external_user_id
        , emeritus_enrollments.user_email
        , null as courserun_id
        , coalesce(
            mitxpro_external_readable_id_lookup.courserun_readable_id
            , emeritus_enrollments.courserun_external_readable_id
        ) as courserun_readable_id
        , null as program_id
        , 'course' as enrollment_scope
        , emeritus_enrollments.enrollment_created_on
        , emeritus_enrollments.enrollment_updated_on
        , emeritus_enrollments.is_enrolled as enrollment_is_active
        , cast(null as varchar) as enrollment_mode
        , emeritus_enrollments.enrollment_status
        , 'emeritus' as platform
        , 'emeritus' as platform_code
        , cast(null as boolean) as enrollment_is_edx_enrolled
        , cast(null as bigint) as order_id
        , cast(null as bigint) as b2b_contract_id
    from {{ ref('stg__emeritus__api__bigquery__user_enrollments') }} as emeritus_enrollments
    left join mitxpro_external_readable_id_lookup
        on emeritus_enrollments.courserun_external_readable_id
            = mitxpro_external_readable_id_lookup.courserun_external_readable_id
)

, global_alumni_enrollments as (
    select
        -- Source has no native enrollment_id. Keyed on the learner the same way as
        -- dim_user.global_alumni_identity_key (email first; a Global Alumni student id
        -- is not unique per person).
        {{ dbt_utils.generate_surrogate_key([
            'coalesce(global_alumni_enrollments.user_email, cast(global_alumni_enrollments.user_id as varchar))',
            'global_alumni_enrollments.courserun_external_readable_id'
        ]) }} as enrollment_id
        , cast(null as bigint) as user_id
        , global_alumni_enrollments.user_id as external_user_id
        , global_alumni_enrollments.user_email
        , null as courserun_id
        , coalesce(
            mitxpro_external_readable_id_lookup.courserun_readable_id
            , global_alumni_enrollments.courserun_external_readable_id
        ) as courserun_readable_id
        , null as program_id
        , 'course' as enrollment_scope
        -- source has no enrollment_created_on/enrollment_updated_on. Unlike edxorg/residential
        -- (which have enrollment_created_on and use the 7-day lookback path), the unconditional
        -- `or ewf.enrollment_created_on is null` branch in incremental_watermarks means every
        -- Global Alumni row is reprocessed on every incremental run (volume is low, so this is
        -- an acceptable tradeoff for now; revisit if an ingestion timestamp becomes available
        -- upstream).
        , cast(null as varchar) as enrollment_created_on
        , cast(null as varchar) as enrollment_updated_on
        , global_alumni_enrollments.is_enrolled as enrollment_is_active
        , cast(null as varchar) as enrollment_mode
        , global_alumni_enrollments.enrollment_status
        , 'global_alumni' as platform
        , 'global_alumni' as platform_code
        , cast(null as boolean) as enrollment_is_edx_enrolled
        , cast(null as bigint) as order_id
        , cast(null as bigint) as b2b_contract_id
    from {{ ref('stg__global_alumni__api__bigquery__user_enrollments') }} as global_alumni_enrollments
    left join mitxpro_external_readable_id_lookup
        on global_alumni_enrollments.courserun_external_readable_id
            = mitxpro_external_readable_id_lookup.courserun_external_readable_id
)

, combined_enrollments as (
    select * from mitxonline_enrollments
    union all
    select * from mitxpro_enrollments
    union all
    select * from edxorg_enrollments
    union all
    select * from residential_enrollments
    union all
    select * from program_enrollments
    union all
    select * from bootcamps_enrollments
    union all
    select * from emeritus_enrollments
    union all
    select * from global_alumni_enrollments
)

-- Join to dimensions for FKs
-- dim_user not in Phase 1-2

, user_lookup as (
    select
        user_pk
        , mitxonline_application_user_id
        , mitxpro_application_user_id
        , edxorg_openedx_user_id
        , micromasters_user_id
        , residential_openedx_user_id
        , bootcamps_application_user_id
        , emeritus_user_id
        , global_alumni_user_id
        , email
    from {{ ref('dim_user') }}
    where user_pk is not null
)

, dim_course_run as (
    select courserun_pk, courserun_readable_id, platform
    from {{ ref('dim_course_run') }}
    where is_current = true
)

-- Emeritus/Global Alumni enrollments matched to an xPro run resolve their course run FK
-- against MITxPro-platform dim_course_run rows (see mitxpro_external_readable_id_lookup
-- above); the rest resolve through the generic platform join.
, dim_course_run_mitxpro as (
    select courserun_pk, courserun_readable_id
    from {{ ref('dim_course_run') }}
    where is_current = true and platform = 'mitxpro'
)

, dim_program as (
    select program_pk, source_id, platform_code
    from {{ ref('dim_program') }}
)

-- MicroMasters program lookup: maps (courserun_readable_id, platform) to micromasters
-- program_pk. Used to enrich edxorg/mitxonline enrollment rows that belong to MM
-- programs. Platform-scoped (not just courserun_readable_id) so a mitxpro/residential/
-- bootcamps enrollment can never inherit an MM program_fk, and deduped one row per
-- (courserun_readable_id, platform) so a course run reused across two MM programs
-- can't fan out the enrollment it's joined against.
, micromasters_program_lookup as (
    select
        courserun_readable_id
        , platform
        , micromasters_program_pk
    from (
        select
            mm_enroll.courserun_readable_id
            , case
                when mm_enroll.platform = '{{ var("edxorg") }}' then 'edxorg'
                when mm_enroll.platform = '{{ var("mitxonline") }}' then 'mitxonline'
            end as platform
            , dim_program.program_pk as micromasters_program_pk
            , row_number() over (
                partition by mm_enroll.courserun_readable_id, mm_enroll.platform
                order by dim_program.program_pk
            ) as _row_num
        from {{ ref('int__micromasters__course_enrollments') }} as mm_enroll
        inner join dim_program
            on cast(mm_enroll.micromasters_program_id as varchar) = cast(dim_program.source_id as varchar)
            and dim_program.platform_code = 'micromasters'
    ) as deduped
    where _row_num = 1
        and platform is not null
)

-- MITx Online B2B contracts. An enrollment records the contract it was made under, and
-- a course run can be offered under several, so the contract is read from the enrollment
-- rather than from the run.
, dim_contract as (
    select contract_pk, contract_id
    from {{ ref('dim_contract') }}
)

-- dim_platform not in Phase 1-2

, dim_platform_lookup as (
    select platform_pk, platform_readable_id
    from {{ ref('dim_platform') }}
)

, enrollments_with_fks as (
    select
        combined_enrollments.*
        , coalesce(
            case when combined_enrollments.platform = 'mitxonline'
                then ul_mitxonline.user_pk
            end,
            case when combined_enrollments.platform = 'mitxpro'
                then ul_mitxpro.user_pk
            end,
            case when combined_enrollments.platform = 'edxorg'
                then ul_edxorg.user_pk
            end,
            case when combined_enrollments.platform = 'residential'
                then ul_residential.user_pk
            end,
            case when combined_enrollments.platform = 'bootcamps'
                then ul_bootcamps.user_pk
            end,
            case when combined_enrollments.platform = 'emeritus'
                then coalesce(ul_emeritus.user_pk, ul_emeritus_email.user_pk)
            end,
            case when combined_enrollments.platform = 'global_alumni'
                then coalesce(ul_global_alumni.user_pk, ul_global_alumni_id.user_pk)
            end
        ) as user_fk
        , coalesce(dim_course_run.courserun_pk, dim_course_run_mitxpro.courserun_pk) as courserun_fk
        , coalesce(dim_program.program_pk, micromasters_program_lookup.micromasters_program_pk) as program_fk
        , dim_platform_lookup.platform_pk as platform_fk
        , dim_contract.contract_pk as contract_fk
        , {{ iso8601_to_date_key('enrollment_created_on') }} as enrollment_date_key
    from combined_enrollments
    left join user_lookup as ul_mitxonline
        on combined_enrollments.platform = 'mitxonline'
        and combined_enrollments.user_id = ul_mitxonline.mitxonline_application_user_id
    left join user_lookup as ul_mitxpro
        on combined_enrollments.platform = 'mitxpro'
        and combined_enrollments.user_id = ul_mitxpro.mitxpro_application_user_id
    left join user_lookup as ul_edxorg
        on combined_enrollments.platform = 'edxorg'
        and combined_enrollments.user_id = ul_edxorg.edxorg_openedx_user_id
    left join user_lookup as ul_residential
        on combined_enrollments.platform = 'residential'
        and combined_enrollments.user_id = ul_residential.residential_openedx_user_id
    left join user_lookup as ul_bootcamps
        on combined_enrollments.platform = 'bootcamps'
        and combined_enrollments.user_id = ul_bootcamps.bootcamps_application_user_id
    -- Emeritus and Global Alumni learners are matched using the same dedup logic as in
    -- dim_user: Emeritus learners by student id, Global Alumni learners by email. Each falls
    -- back to the other method if the primary lookup finds no one. The email fallback can't
    -- use dim_user's identity key: if a learner has a mix of enrollments, some with ids and
    -- some without, their key is the student id, so the rows without an id would miss it.
    left join user_lookup as ul_emeritus
        on combined_enrollments.platform = 'emeritus'
        and combined_enrollments.external_user_id = ul_emeritus.emeritus_user_id
    left join user_lookup as ul_emeritus_email
        on combined_enrollments.platform = 'emeritus'
        and lower(combined_enrollments.user_email) = ul_emeritus_email.email
    left join user_lookup as ul_global_alumni
        on combined_enrollments.platform = 'global_alumni'
        and lower(combined_enrollments.user_email) = ul_global_alumni.email
    left join user_lookup as ul_global_alumni_id
        on combined_enrollments.platform = 'global_alumni'
        and combined_enrollments.external_user_id = ul_global_alumni_id.global_alumni_user_id
    left join dim_course_run
        on combined_enrollments.courserun_readable_id = dim_course_run.courserun_readable_id
        and combined_enrollments.platform = dim_course_run.platform
    left join dim_course_run_mitxpro
        on combined_enrollments.platform in ('emeritus', 'global_alumni')
        and combined_enrollments.courserun_readable_id = dim_course_run_mitxpro.courserun_readable_id
    left join dim_program
        on cast(combined_enrollments.program_id as varchar) = dim_program.source_id
        and combined_enrollments.platform_code = dim_program.platform_code
    left join micromasters_program_lookup
        on combined_enrollments.courserun_readable_id = micromasters_program_lookup.courserun_readable_id
        and combined_enrollments.platform = micromasters_program_lookup.platform
    left join dim_platform_lookup
        on combined_enrollments.platform = dim_platform_lookup.platform_readable_id
    left join dim_contract
        on combined_enrollments.platform = 'mitxonline'
        and combined_enrollments.b2b_contract_id = dim_contract.contract_id
)

{% if is_incremental() %}
-- Pre-compute per-(platform, enrollment_type) watermarks to avoid the correlated
-- subquery anti-pattern. Without this, Trino executes the WHERE as:
--   AssignUniqueId + LeftJoin(all target rows on platform+type) + StreamingAggregate
-- which fans out to billions of intermediate rows.
-- Use max(enrollment_updated_on) where available (program enrollments), falling back
-- to max(enrollment_created_on), so status/mode changes are captured incrementally.
, incremental_watermarks as (
    select
        platform as watermark_platform
        , enrollment_type as watermark_enrollment_type
        , max(coalesce(enrollment_updated_on, enrollment_created_on)) as max_activity_on
    from {{ this }}
    group by platform, enrollment_type
)

-- Snapshot of the target's current foreign keys, used to re-select rows whose user_fk has
-- gone stale after a dim_user re-key, whose courserun_fk has changed (e.g. an Emeritus run
-- code that gains an xPro match), or whose contract_fk has (MITx Online backfilled the
-- enrollment's contract without moving updated_on past our watermark). The source row
-- itself did not change, so the activity-timestamp watermark alone would never catch these.
, stale_fk_lookup as (
    select
        enrollment_key
        , user_fk as stored_user_fk
        , courserun_fk as stored_courserun_fk
        , {% if target_has_contract_fk %}contract_fk{% else %}cast(null as varchar){% endif %} as stored_contract_fk
    from {{ this }}
)
{% endif %}

, final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'cast(enrollment_id as varchar)',
            'platform',
            'enrollment_scope'
        ]) }} as enrollment_key
        , ewf.enrollment_id
        , ewf.enrollment_scope as enrollment_type
        , ewf.enrollment_date_key
        , ewf.user_fk
        , ewf.courserun_fk
        , ewf.program_fk
        , ewf.platform_fk
        , ewf.contract_fk
        , ewf.platform
        , ewf.enrollment_is_active
        , ewf.enrollment_mode
        , ewf.enrollment_status
        , ewf.enrollment_created_on
        , ewf.enrollment_updated_on
        , ewf.enrollment_is_edx_enrolled
        , ewf.order_id
    from enrollments_with_fks as ewf

    {% if is_incremental() %}
    -- left join preserves enrollments from platforms/types not yet in the target table
    left join incremental_watermarks w
        on w.watermark_platform = ewf.platform
        and w.watermark_enrollment_type = ewf.enrollment_scope
    left join stale_fk_lookup as sfk
        on sfk.enrollment_key = {{ dbt_utils.generate_surrogate_key([
            "cast(ewf.enrollment_id as varchar)",
            "ewf.platform",
            "ewf.enrollment_scope"
        ]) }}
    where (
        w.max_activity_on is null  -- platform/type not yet in target, include all
        -- Use >= for updated_on watermark: updated_on can equal max on state changes within same second
        or coalesce(ewf.enrollment_updated_on, ewf.enrollment_created_on) >= w.max_activity_on
        -- For platforms without updated_on (edxorg, residential), apply a 7-day lookback to catch
        -- status changes (deactivations, mode upgrades) that don't bump enrollment_created_on
        or (
            ewf.enrollment_updated_on is null
            and ewf.enrollment_created_on is not null
            and ewf.enrollment_created_on >= {{ cast_timestamp_to_iso8601("current_timestamp - interval '7' day") }}
        )
        or ewf.enrollment_created_on is null
        -- dim_user re-key: re-select rows whose resolved user_fk no longer matches the target
        or sfk.stored_user_fk is distinct from ewf.user_fk
        -- course run re-resolved: re-select rows whose resolved courserun_fk no longer matches
        or sfk.stored_courserun_fk is distinct from ewf.courserun_fk
        -- contract added or re-resolved, including every B2B row on the run that adds the column
        or sfk.stored_contract_fk is distinct from ewf.contract_fk
    )
    {% endif %}
)

-- No dedup here: each lookup above is unique on its join key, so a duplicate enrollment_key
-- means a source or a lookup lost its grain. The error-severity unique test on
-- enrollment_key reports it. A row_number here would keep one copy's FKs at random.
select
    enrollment_key
    , enrollment_id
    , enrollment_type
    , enrollment_date_key
    , user_fk
    , courserun_fk
    , program_fk
    , platform_fk
    , contract_fk
    , platform
    , enrollment_is_active
    , enrollment_mode
    , enrollment_status
    , enrollment_created_on
    , enrollment_updated_on
    , enrollment_is_edx_enrolled
    , order_id
from final
