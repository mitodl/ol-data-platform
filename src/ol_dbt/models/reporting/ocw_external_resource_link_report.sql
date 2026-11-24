with resources as (
    select * from {{ ref('dim_ocw_resource') }}
    where content_type = 'external-resource'
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
    -- ocw-studio's link checker sets status; resources it has never checked have none.
    , coalesce(resources.external_resource_status, 'not_checked') as link_status
    , coalesce(resources.external_resource_status = 'broken', false) as is_link_broken
    , coalesce(resources.external_resource_license_warning, false) as has_license_warning
    , coalesce(
        resources.external_resource_status in ('broken', 'check_failed')
        or resources.external_resource_license_warning
        , false
    ) as needs_review
from resources
left join courses
    on resources.course_name = courses.course_name
