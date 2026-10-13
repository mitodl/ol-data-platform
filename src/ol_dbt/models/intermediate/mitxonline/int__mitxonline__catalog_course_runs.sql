-- Every MITx Online course run as its catalog API for MIT Learn (/api/internal/courses/)
-- lists it under a course, source and B2B runs included, with the state MIT Learn's ETL
-- (learning_resources/etl/mitxonline.py) derives per run. is_enrollable follows
-- CourseRun.is_enrollable in mitxonline's courses/models.py and is_fully_enrollable follows
-- MIT Learn's is_fully_enrollable, both evaluated when this model builds.

with runs as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_courserun') }}
)

, run_modes as (
    select
        run_modes.courserun_id
        , array_agg(modes.enrollmentmode_slug order by modes.enrollmentmode_slug) as courserun_enrollment_modes
        , max(case when modes.enrollmentmode_slug = 'verified' then 1 else 0 end) = 1 as courserun_has_verified_mode
        , max(case when modes.enrollmentmode_slug = 'audit' then 1 else 0 end) = 1 as courserun_has_audit_mode
    from {{ ref('stg__mitxonline__app__postgres__courses_courserun_enrollment_modes') }} as run_modes
    inner join {{ ref('stg__mitxonline__app__postgres__courses_enrollmentmode') }} as modes
        on run_modes.enrollmentmode_id = modes.enrollmentmode_id
    group by run_modes.courserun_id
)

-- A course's variant sets. b2b.contractpage rows share object ids with courses, so the
-- content type has to match. MIT Learn takes the first set with the run's language; the API
-- does not order them, so the lowest id stands in for first.
, course_variants as (
    select
        variants.supportedvariant_object_id as course_id
        , variants.supportedvariant_language
        , variants.supportedvariant_is_default
        , row_number() over (
            partition by variants.supportedvariant_object_id, variants.supportedvariant_language
            order by variants.supportedvariant_id
        ) as variant_rank
    from {{ ref('stg__mitxonline__app__postgres__variants_supportedvariant') }} as variants
    inner join {{ ref('stg__mitxonline__app__postgres__django_contenttype') }} as content_types
        on variants.contenttype_id = content_types.contenttype_id
    where content_types.contenttype_full_name = 'courses_course'
)

, enrollable as (
    select
        runs.*
        , (
            (
                runs.courserun_enrollment_end_on is null
                or {{ from_iso8601_timestamp('runs.courserun_enrollment_end_on') }} > current_timestamp
            )
            and runs.courserun_enrollment_start_on is not null
            and {{ from_iso8601_timestamp('runs.courserun_enrollment_start_on') }} <= current_timestamp
            and runs.courserun_is_live
            and runs.courserun_start_on is not null
        ) as courserun_is_enrollable
    from runs
)

select
    enrollable.courserun_id
    , enrollable.course_id
    , enrollable.courserun_readable_id
    , enrollable.courserun_title
    , enrollable.courserun_start_on
    , enrollable.courserun_end_on
    , enrollable.courserun_enrollment_start_on
    , enrollable.courserun_enrollment_end_on
    , enrollable.courserun_is_live
    , enrollable.courserun_is_self_paced
    , enrollable.courserun_is_source_run
    , enrollable.courserun_language
    , enrollable.courserun_is_enrollable
    -- opaque-keys' CourseKey.course: the second part of course-v1:org+course+run or of
    -- org/course/run
    , coalesce(
        {{ element_at_array("split(enrollable.courserun_readable_id, '+')", 2) }}
        , {{ element_at_array("split(enrollable.courserun_readable_id, '/')", 2) }}
        , enrollable.courserun_readable_id
    ) as courserun_course_number
    , coalesce(enrollable.courserun_is_b2b_only, false) as courserun_is_b2b_only
    , (
        coalesce(enrollable.courserun_is_b2b_only, false)
        or coalesce(not course_variants.supportedvariant_is_default, false)
    ) as courserun_is_variant
    , (
        enrollable.courserun_is_enrollable
        and (
            coalesce(enrollable.courserun_enrollment_end_on, enrollable.courserun_end_on) is null
            or {{ from_iso8601_timestamp(
                'coalesce(enrollable.courserun_enrollment_end_on, enrollable.courserun_end_on)'
            ) }} >= current_timestamp
        )
    ) as courserun_is_fully_enrollable
    , run_modes.courserun_enrollment_modes
    , coalesce(run_modes.courserun_has_verified_mode, false) as courserun_has_verified_mode
    , coalesce(run_modes.courserun_has_audit_mode, false) as courserun_has_audit_mode
from enrollable
left join run_modes on enrollable.courserun_id = run_modes.courserun_id
left join course_variants
    on
        enrollable.course_id = course_variants.course_id
        and enrollable.courserun_language = course_variants.supportedvariant_language
        and course_variants.variant_rank = 1
