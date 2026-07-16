with combined_enrollments as (
    select * from {{ ref('int__combined__courserun_enrollments') }}
)

, retired_users as (
    select cast(user_id as varchar) as user_id
    from {{ ref('int__edxorg__mitx_users') }}
    where
        user_is_active = false
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
        , array_distinct(
            array_agg(signatory_normalized_name order by signatory_normalized_name)
            filter (where signatory_normalized_name <> '')
          ) as signatory_names
        , array_sort(
            array_distinct(
                array_agg(mitxonline_signatory.wagtail_page_id)
                filter (where mitxonline_signatory.wagtail_page_id is not null)
            )
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

, migrated_certificate_revision as (
    -- Fallback source for certificate_page_revision_id when signatory names don't match
    -- exactly between edX and MITx Online: use the highest wagtailcore_revision_id recorded
    -- for any certificate on the same course run in MITx Online, one row per courserun_id.
    select
        courserun_id
        , max(wagtailcore_revision_id) as wagtailcore_revision_id
    from {{ ref('stg__mitxonline__app__postgres__courses_courseruncertificate') }}
    where wagtailcore_revision_id is not null
    group by courserun_id
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

, future_run_id_mapping as (
    -- Map the known future edX.org runs (no certificate yet) to their MITx Online
    -- counterparts by matching course number and run tag, ignoring org prefix differences
    -- (e.g. MITx vs MITxT). edX.org format: org/course/run; MITx Online: course-v1:org+course+run.
    select
        {{ format_course_id('edxorg_runs.courserun_readable_id', false) }} as edxorg_courserun_readable_id
        , mitxonline__course_runs.courserun_readable_id as mitxonline_courserun_readable_id
    from edxorg_runs
    inner join mitxonline__course_runs
        on {{ element_at_array("split(edxorg_runs.courserun_readable_id, '/')", 2) }}
            = {{ element_at_array("split(mitxonline__course_runs.courserun_readable_id, '+')", 2) }}
        and {{ element_at_array("split(edxorg_runs.courserun_readable_id, '/')", 3) }}
            = {{ element_at_array("split(mitxonline__course_runs.courserun_readable_id, '+')", 3) }}
    where edxorg_runs.courserun_readable_id in (
        'MITx/15.763x/2T2026',
        'MITx/18.6501x/3T2026',
        'MITx/2.830.1x/2T2026',
        'MITx/2.830.2x/3T2026',
        'MITx/2.854.2x/1T2027',
        'MITx/2.961.1x/3T2026',
        'MITx/2.961.2x/1T2027',
        'MITx/6.419x/1T2027',
        'MITx/6.431x/3T2026',
        'MITx/6.86x/1T2027',
        'MITx/CTL.SC2x/2T2026',
        'MITx/CTL.SC3x/3T2026',
        'MITx/CTL.SC4x/2T2026',
        'MITx/IDS.S24x/1T2027'
    )
        and mitxonline__course_runs.courserun_platform = '{{ var("mitxonline") }}'

    union all

    -- Special case: MITx/CTL.SC1x_1/3T2026 maps to course-v1:MITxT+CTL.SC1x+3T2026.
    -- The edX course number has a _1 suffix that is absent in the MITx Online counterpart,
    -- so the standard course/run tag matching logic above cannot resolve this automatically.
    select
        'course-v1:MITx+CTL.SC1x_1+3T2026' as edxorg_courserun_readable_id
        , courserun_readable_id as mitxonline_courserun_readable_id
    from mitxonline__course_runs
    where courserun_readable_id = 'course-v1:MITxT+CTL.SC1x+3T2026'
        and courserun_platform = '{{ var("mitxonline") }}'
)

, edxorg_future_enrollment as (
    -- For future (not-yet-run) edX.org course runs, enrollment_mode/created_on are sourced
    -- from mitx_user_info_combo instead of mitx_person_course (via edxorg_enrollment/
    -- combined_enrollments)
    -- Certificate data continues to source from both tables as before.
    select
        cast(user_id as varchar) as user_id
        , courserunenrollment_courserun_readable_id as courserun_readable_id
        , courserunenrollment_created_on
        , courserunenrollment_mode as courserunenrollment_enrollment_mode
        , user_email
    from {{ ref('stg__edxorg__bigquery__mitx_user_info_combo') }}
    where
        courserun_platform = '{{ var("edxorg") }}'
        and courserunenrollment_courserun_readable_id is not null
)

, product_versions as (
    select version_id, version_object_id
    from {{ ref('stg__mitxonline__app__postgres__reversion_version') }}
    where contenttype_id in (
        select contenttype_id
        from {{ ref('stg__mitxonline__app__postgres__django_contenttype') }}
        where contenttype_full_name = 'ecommerce_product'
    )
)

, courserun_product_version as (
    select
        mitxonline_products.courserun_id
        , min(product_versions.version_id) as product_version_id
    from future_run_id_mapping
    inner join {{ ref('int__mitxonline__ecommerce_product') }} as mitxonline_products
        on future_run_id_mapping.mitxonline_courserun_readable_id = mitxonline_products.courserun_readable_id
    inner join product_versions
        on mitxonline_products.product_id = product_versions.version_object_id
    group by mitxonline_products.courserun_id
)

, purchased_on_edx_discount as (
    select discount_id
    from {{ ref('int__mitxonline__ecommerce_discount') }}
    where discount_code = 'purchased-on-edx'
    limit 1
)

, mitxonline_enrollment as (
    select * from {{ ref('int__mitxonline__courserunenrollments') }}
)

, openedx_users as (
    select user_id, openedxuser_has_been_synced
    from {{ ref('stg__mitxonline__app__postgres__openedx_openedxuser') }}
)

, future_run_user_info_combo_enrollments as (
    -- mitx_user_info_combo rows for the same future-run course list. mitx_person_course
    -- (combined_enrollments' upstream source) may not have activity data yet for a learner who
    -- has enrolled but not yet interacted with a not-yet-started course, so this is the only
    -- place those enrollments exist.
    select
        edxorg_future_enrollment.user_id
        , edxorg_future_enrollment.courserun_readable_id
        , edxorg_future_enrollment.courserunenrollment_created_on
        , edxorg_future_enrollment.courserunenrollment_enrollment_mode
        , edxorg_future_enrollment.user_email
    from edxorg_future_enrollment
    inner join future_run_id_mapping
        on {{ format_course_id('edxorg_future_enrollment.courserun_readable_id', false) }}
            = future_run_id_mapping.edxorg_courserun_readable_id
    -- Exclude retired users
    where edxorg_future_enrollment.user_email not like 'retired__user%'
)

, edxorg_enrollment as (
    -- Certificate-bearing enrollments: the course already ran, so future-run overrides never
    -- apply here (courseruncertificate_created_on is not null below).
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
    from combined_enrollments
    left join edxorg_runs
       on combined_enrollments.courserun_readable_id = edxorg_runs.courserun_readable_id
    where combined_enrollments.platform = '{{ var("edxorg") }}'
    and combined_enrollments.courseruncertificate_created_on is not null
    --- exclude DEDP Micromasters program courses as those are already migrated
    and (edxorg_runs.micromasters_program_id != {{ var("dedp_micromasters_program_id") }}
    or edxorg_runs.micromasters_program_id is null)
    --- Exclude PEx runs as these are transient and don't need to be migrated. Anyone who earned a certificate
      -- in them also earned a certificate in the corresponding non-PEx version of the course
    and combined_enrollments.courserun_readable_id not like '%PEx%'
    -- Exclude MITx/15.390.1x_SPA/1T1015. In edX it uses the legacy course ID
    --   source (edx.org) courserun_readable_id: MITx/15.390.1x_SPA/1T1015
    --   normalized (MITx Online) course-v1 ID: course-v1:MITx+15.390.1x_SPA+1T2015
    -- This run has already been migrated under the normalized ID, so we exclude
    -- the legacy ID here to avoid attempting a duplicate migration with the
    -- outdated courserun_readable_id.
    and combined_enrollments.courserun_readable_id != 'MITx/15.390.1x_SPA/1T1015'
    -- Exclude retired users
    and combined_enrollments.user_email not like 'retired__user%'
    and combined_enrollments.user_username not like 'retired__user%'

    union all

    -- Future (not-yet-run) enrollments: sourced entirely from mitx_user_info_combo
    -- (edxorg_future_enrollment/future_run_user_info_combo_enrollments) -- mitx_person_course
    -- (combined_enrollments' upstream source) may lack activity data for a learner who enrolled
    -- but hasn't interacted with a not-yet-started course yet, so this doesn't require the row to
    -- already exist in combined_enrollments. This is independent of the certificate branch above:
    -- a user can have both a certificate row (from combined_enrollments) and a future-enrollment
    -- row (from mitx_user_info_combo) for the same course, e.g. once a course starts issuing
    -- certificates while briefly still on the hardcoded future-run list -- see the
    -- courseruncertificate_created_on-inclusive uniqueness test on this model.
    select
        future_run_user_info_combo_enrollments.courserunenrollment_created_on
        , future_run_user_info_combo_enrollments.courserunenrollment_enrollment_mode
        , future_run_user_info_combo_enrollments.user_id
        , {{ format_course_id('future_run_user_info_combo_enrollments.courserun_readable_id', false) }}
            as courserun_readable_id
        , null as course_readable_id
        , future_run_user_info_combo_enrollments.user_email
        , null as courseruncertificate_created_on
        , null as courserungrade_grade
        , null as courserungrade_is_passing
    from future_run_user_info_combo_enrollments
)

select
    edxorg_enrollment.user_id as user_edxorg_id
    , coalesce(mitx_users_by_email.user_mitxonline_id, mitx__users.user_mitxonline_id) as user_mitxonline_id
    , edxorg_enrollment.user_email
    , coalesce(mitx_users_by_email.user_full_name, mitx__users.user_full_name) as user_full_name
    , coalesce(mitx_users_by_email.user_gender, mitx__users.user_gender) as user_gender
    , coalesce(mitx_users_by_email.user_birth_year, mitx__users.user_birth_year) as user_birth_year
    , coalesce(
        mitx_users_by_email.user_address_country, mitx__users.user_address_country
    ) as user_country
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
    , coalesce(
        edx_to_mitxonline_certificate_revision.wagtailcore_revision_id
        , migrated_certificate_revision.wagtailcore_revision_id
     ) as certificate_page_revision_id
    , edx_signatories.signatory_names
    , case
        when future_run_id_mapping.edxorg_courserun_readable_id is not null
            and edxorg_enrollment.courserunenrollment_enrollment_mode = 'verified'
            and edxorg_enrollment.courseruncertificate_created_on is null
            then courserun_product_version.product_version_id
    end as product_version_id
    , case
        when future_run_id_mapping.edxorg_courserun_readable_id is not null
            and edxorg_enrollment.courserunenrollment_enrollment_mode = 'verified'
            and edxorg_enrollment.courseruncertificate_created_on is null
            then purchased_on_edx_discount.discount_id
    end as discount_id
    , openedx_users.openedxuser_has_been_synced
from edxorg_enrollment
left join edxorg_grade
    on edxorg_enrollment.user_id = edxorg_grade.user_id
    and edxorg_enrollment.courserun_readable_id = edxorg_grade.courserun_readable_id
left join future_run_id_mapping
    on edxorg_enrollment.courserun_readable_id = future_run_id_mapping.edxorg_courserun_readable_id
left join mitxonline_enrollment
    on lower(edxorg_enrollment.user_email) = lower(mitxonline_enrollment.user_email)
    and coalesce(
        future_run_id_mapping.mitxonline_courserun_readable_id
        , edxorg_enrollment.courserun_readable_id
    ) = mitxonline_enrollment.courserun_readable_id
left join mitxonline__course_runs
    on coalesce(
        future_run_id_mapping.mitxonline_courserun_readable_id
        , edxorg_enrollment.courserun_readable_id
    ) = mitxonline__course_runs.courserun_readable_id
left join mitx__users
       on edxorg_enrollment.user_id = cast(mitx__users.user_edxorg_id as varchar)
left join mitx__users as mitx_users_by_email
       on lower(edxorg_enrollment.user_email) = lower(mitx_users_by_email.user_mitxonline_email)
left join mitxonline_enrollment as mitxonline_enrollment_by_userid
    on coalesce(mitx_users_by_email.user_mitxonline_id, mitx__users.user_mitxonline_id)
        = mitxonline_enrollment_by_userid.user_id
    and coalesce(
        future_run_id_mapping.mitxonline_courserun_readable_id
        , edxorg_enrollment.courserun_readable_id
    ) = mitxonline_enrollment_by_userid.courserun_readable_id
left join edx_to_mitxonline_certificate_revision
    on edxorg_enrollment.courserun_readable_id = edx_to_mitxonline_certificate_revision.courserun_readable_id
left join migrated_certificate_revision
    on mitxonline__course_runs.courserun_id = migrated_certificate_revision.courserun_id
left join edx_signatories
    on edxorg_enrollment.courserun_readable_id = edx_signatories.courserun_readable_id
left join retired_users
    on edxorg_enrollment.user_id = retired_users.user_id
left join courserun_product_version
    on mitxonline__course_runs.courserun_id = courserun_product_version.courserun_id
left join purchased_on_edx_discount on true
left join openedx_users
    on coalesce(mitx_users_by_email.user_mitxonline_id, mitx__users.user_mitxonline_id) = openedx_users.user_id
where
    (
        edxorg_enrollment.courseruncertificate_created_on is not null
        or future_run_id_mapping.edxorg_courserun_readable_id is not null
    )
    and (
        (mitxonline_enrollment.user_email is null and mitxonline_enrollment_by_userid.user_id is null)
        -- Include future-run enrollments where edX mode was upgraded to verified but the
        -- existing MITx Online enrollment is still audit
        or (
            future_run_id_mapping.edxorg_courserun_readable_id is not null
            and edxorg_enrollment.courserunenrollment_enrollment_mode = 'verified'
            and coalesce(
                mitxonline_enrollment.courserunenrollment_enrollment_mode
                , mitxonline_enrollment_by_userid.courserunenrollment_enrollment_mode
            ) = 'audit'
        )
    )
    and retired_users.user_id is null
