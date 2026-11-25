with resources as (
    select
        *
        -- The checker's state records a check that failed before a response; metadata keeps the
        -- last good status then, so the state wins and metadata only fills in when there is none.
        -- A new resource's state starts as unchecked with no check time until the weekly run.
        , case
            when
                external_resource_check_status = 'unchecked'
                and external_resource_last_checked_on is null
                then 'not_checked'
            else coalesce(external_resource_check_status, external_resource_status, 'not_checked')
        end as link_status
        , coalesce(trim(external_resource_url), '') = '' as is_url_missing
    from {{ ref('dim_ocw_resource') }}
    where
        content_type = 'external-resource'
        and resource_deleted_on is null
)

, courses as (
    select * from {{ ref('dim_ocw_course') }}
)

select
    resources.course_uuid
    , resources.course_name
    , resources.course_number
    , resources.course_title
    , resources.course_term
    , resources.course_year
    , resources.course_live_url
    , courses.course_is_unpublished
    , courses.course_has_never_published
    , resources.resource_uuid
    , resources.resource_title
    , resources.resource_live_url
    , resources.studio_url
    , resources.external_resource_url
    , resources.external_resource_url_status_code
    , resources.external_resource_wayback_url
    , resources.external_resource_last_checked_on
    , resources.link_status
    , resources.link_status = 'broken' as is_link_broken
    , resources.is_url_missing
    , coalesce(resources.external_resource_license_warning, false) as has_license_warning
    , (
        resources.link_status in ('broken', 'check_failed')
        or resources.is_url_missing
        or coalesce(resources.external_resource_license_warning, false)
    ) as needs_review
from resources
left join courses
    on resources.course_name = courses.course_name
