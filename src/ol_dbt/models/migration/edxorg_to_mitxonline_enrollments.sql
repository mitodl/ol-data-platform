with combined_enrollments as (
    select * from {{ ref('int__combined__courserun_enrollments') }}
)

, program_entitlements as (
    select *
    from {{ ref('stg__edxorg__program_entitlement') }}
)

, program_courses as (
    select * from {{ ref('int__edxorg__mitx_program_courses') }}
)

, edxorg_grade as (
    select
          {{ format_course_id('courseruncertificate_courserun_readable_id', false) }} as courserun_readable_id
         , cast(user_id as varchar) as user_id
         , courseruncertificate_grade
    from {{ ref('stg__edxorg__bigquery__mitx_user_info_combo') }}
    where
        courserun_platform = '{{ var("edxorg") }}'
        and courseruncertificate_grade is not null
)

, edxorg_runs as (
    select * from {{ ref('int__edxorg__mitx_courseruns') }}
)

, mitx__users as (
    select * from {{ ref('int__mitx__users') }}
)

, mitxonline__course_runs as (
    select * from {{ ref('int__mitxonline__course_runs') }}
)

, course_pages as (
    select * from {{ ref('stg__mitxonline__app__postgres__cms_coursepage') }}
)

, wagtail_page as (
    select * from {{ ref('stg__mitxonline__app__postgres__cms_wagtail_page') }}
)

, missing_signatories_revision_mapping as (
    select * from {{ ref('legacy_edx_certificate_revision_mapping') }}
)

, mitxonline_certificate_page as (

   select
       mitxonline__course_runs.courserun_id
       , mitxonline__course_runs.courserun_readable_id
       , certificate_page.wagtail_page_id as certificate_page_id
   from mitxonline__course_runs
    join course_pages
        on mitxonline__course_runs.course_id = course_pages.course_id
    join wagtail_page
       on course_pages.wagtail_page_id = wagtail_page.wagtail_page_id
   join wagtail_page as certificate_page
       on certificate_page.wagtail_page_path like wagtail_page.wagtail_page_path || '%'
        and certificate_page.wagtail_page_path <> wagtail_page.wagtail_page_path
        and certificate_page.wagtail_page_slug like 'certificate%'

)

, edx_signatories as (
    select
        courserun_readable_id
        , array_agg(signatory_normalized_name order by signatory_normalized_name)
            filter (where signatory_normalized_name <> '') as signatory_names
        , array_sort(
            array_agg(mitxonline_signatory.wagtail_page_id)
            filter (where mitxonline_signatory.wagtail_page_id is not null)
          ) AS signatory_ids
    from {{ ref('stg__edxorg__s3__course_certificate_signatory') }} edx_signatory
    left join {{ ref('stg__mitxonline__app__postgres__cms_signatorypage') }} as mitxonline_signatory
        on edx_signatory.signatory_normalized_name = mitxonline_signatory.signatorypage_name
    group by courserun_readable_id
)

, mitxonline_certificate_revision as (
    select
        revision.wagtailcore_revision_id
        , cast(revision.wagtail_page_id as bigint) as certificate_page_id
        , array_sort(
            transform(
                cast(
                 json_parse({{ json_query_string('wagtailcore_revision_content', "'$.signatories'") }}) as array(json)
                )
                , x ->  CAST(json_extract_scalar(x, '$.value') as integer)
            )
        ) AS signatory_ids
    from {{ ref('stg__mitxonline__app__postgres__cms_wagtailcore_revision') }} as revision
    inner join {{ ref('stg__mitxonline__app__postgres__django_contenttype') }} as contenttype
        on revision.contenttype_id = contenttype.contenttype_id
    where contenttype.contenttype_full_name = 'cms_certificatepage'
)

, ranked_revisions AS (
    select
        certificate_page_id
        , signatory_ids
        , wagtailcore_revision_id
        , row_number() over (
            partition by certificate_page_id, signatory_ids
            order by wagtailcore_revision_id desc
        ) as row_number
    from mitxonline_certificate_revision
)

, edx_to_mitxonline_certificate_revision as (
    select distinct
        edx_signatories.courserun_readable_id
        , edx_signatories.signatory_names
        , ranked_revisions.wagtailcore_revision_id
    from edx_signatories
    inner join mitxonline_certificate_page
        on mitxonline_certificate_page.courserun_readable_id = edx_signatories.courserun_readable_id
    inner join ranked_revisions
        on mitxonline_certificate_page.certificate_page_id = ranked_revisions.certificate_page_id
        and edx_signatories.signatory_ids = ranked_revisions.signatory_ids
        and ranked_revisions.row_number = 1

    union all

    select
        courserun_readable_id
        , null as signatory_names
        , certificate_page_revision_id
    from missing_signatories_revision_mapping
)

, mitxonline_enrollment as (
    select * from {{ ref('int__mitxonline__courserunenrollments') }}
)

, course_enrollments as (
    select
        combined_enrollments.courserunenrollment_created_on
        , combined_enrollments.courserunenrollment_enrollment_mode
        , combined_enrollments.user_id
        , {{ format_course_id('combined_enrollments.courserun_readable_id', false) }} as courserun_readable_id
        , combined_enrollments.course_readable_id
        , combined_enrollments.user_email
        , combined_enrollments.courseruncertificate_created_on
        , combined_enrollments.courserungrade_grade
        , combined_enrollments.courserungrade_is_passing
        , false as is_program_entitlement_enrollment
    from combined_enrollments
    left join edxorg_runs
       on combined_enrollments.courserun_readable_id = edxorg_runs.courserun_readable_id
    where combined_enrollments.platform = '{{ var("edxorg") }}'
    --- exclude DEDP Micromasters program courses as those are already migrated
    and (edxorg_runs.micromasters_program_id != {{ var("dedp_micromasters_program_id") }}
    or edxorg_runs.micromasters_program_id is null)
    --- Exclude PEx runs as these are transient and don't need to be migrated. Anyone who earned a certificate
      -- in them also earned a certificate in the corresponding non-PEx version of the course
    and combined_enrollments.courserun_readable_id not like '%PEx%'
    -- Exclude retired users
    and combined_enrollments.user_email not like 'retired__user%'
    and combined_enrollments.user_username not like 'retired__user%'
)

, entitlement_enrollments as (
    select distinct
        combined_enrollments.courserunenrollment_created_on
        , combined_enrollments.courserunenrollment_enrollment_mode
        , combined_enrollments.user_id
        , {{ format_course_id('combined_enrollments.courserun_readable_id', false) }} as courserun_readable_id
        , combined_enrollments.course_readable_id
        , combined_enrollments.user_email
        , combined_enrollments.courseruncertificate_created_on
        , combined_enrollments.courserungrade_grade
        , combined_enrollments.courserungrade_is_passing
        , true as is_program_entitlement_enrollment
    from combined_enrollments
    join edxorg_runs
          on combined_enrollments.courserun_readable_id = edxorg_runs.courserun_readable_id
    join program_courses
          on edxorg_runs.course_readable_id = program_courses.course_readable_id
    join program_entitlements
       on lower(combined_enrollments.user_email) = program_entitlements.user_email
        and cast(combined_enrollments.user_id as varchar) = cast(program_entitlements.user_id as varchar)
        and program_courses.program_title = program_entitlements.program_title
    where combined_enrollments.platform = '{{ var("edxorg") }}'
      and combined_enrollments.courserun_readable_id not like '%PEx%'
      and combined_enrollments.user_email not like 'retired__user%'
      and combined_enrollments.user_username not like 'retired__user%'
)

, edxorg_enrollment as (
    select * from course_enrollments
    union distinct
    select * from entitlement_enrollments
)

select
    edxorg_enrollment.user_id as user_edxorg_id
    , coalesce(mitx_users_by_email.user_mitxonline_id, mitx__users.user_mitxonline_id) as user_mitxonline_id
    , edxorg_enrollment.user_email
    , mitxonline__course_runs.courserun_id
    , edxorg_enrollment.courserun_readable_id
    , edxorg_enrollment.courserunenrollment_enrollment_mode
    , coalesce(
        edxorg_enrollment.courserungrade_grade,
        edxorg_grade.courseruncertificate_grade
     ) as courserungrade_grade
    , edxorg_enrollment.courserungrade_is_passing
    , edxorg_enrollment.courserunenrollment_created_on
    , edxorg_enrollment.courseruncertificate_created_on
    , edx_to_mitxonline_certificate_revision.wagtailcore_revision_id as certificate_page_revision_id
    , edx_signatories.signatory_names
from edxorg_enrollment
left join edxorg_grade
    on edxorg_enrollment.user_id = edxorg_grade.user_id
    and edxorg_enrollment.courserun_readable_id = edxorg_grade.courserun_readable_id
left join mitxonline_enrollment
    on
        lower(edxorg_enrollment.user_email) = lower(mitxonline_enrollment.user_email)
       and edxorg_enrollment.courserun_readable_id = mitxonline_enrollment.courserun_readable_id
left join mitxonline__course_runs
    on edxorg_enrollment.courserun_readable_id = mitxonline__course_runs.courserun_readable_id
left join mitx__users
       on edxorg_enrollment.user_id = cast(mitx__users.user_edxorg_id as varchar)
left join mitx__users as mitx_users_by_email
       on lower(edxorg_enrollment.user_email) = lower(mitx_users_by_email.user_mitxonline_email)
left join edx_to_mitxonline_certificate_revision
    on edxorg_enrollment.courserun_readable_id = edx_to_mitxonline_certificate_revision.courserun_readable_id
left join edx_signatories
    on edxorg_enrollment.courserun_readable_id = edx_signatories.courserun_readable_id
where
    (
      edxorg_enrollment.courseruncertificate_created_on is not null
      or edxorg_enrollment.is_program_entitlement_enrollment
    )
    and mitxonline_enrollment.user_email is null
    and mitx__users.user_mitxonline_email is null
