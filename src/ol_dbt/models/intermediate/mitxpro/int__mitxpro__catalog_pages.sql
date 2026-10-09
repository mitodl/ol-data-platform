-- The CMS page of each xPRO course and program, with the fields xPRO's catalog API reads off
-- it: the thumbnail image, the CEUs of the page's certificate child page, and the names on its
-- faculty child page.

with pages as (
    select * from {{ ref('stg__mitxpro__app__postgres__wagtail_page') }}
)

, images as (
    select * from {{ ref('stg__mitxpro__app__postgres__wagtailimages_image') }}
)

, product_pages as (
    select
        course_id
        , cast(null as bigint) as program_id
        , wagtail_page_id
        , cms_coursepage_description as page_description
        , cms_coursepage_duration as page_duration
        , cms_coursepage_format as page_format
        , cms_coursepage_time_commitment as page_time_commitment
        , cms_coursepage_thumbnail_image_id as page_thumbnail_image_id
        , cms_coursepage_min_weeks as page_min_weeks
        , cms_coursepage_max_weeks as page_max_weeks
        , cms_coursepage_min_weekly_hours as page_min_weekly_hours
        , cms_coursepage_max_weekly_hours as page_max_weekly_hours
    from {{ ref('stg__mitxpro__app__postgres__cms_coursepage') }}
    union all
    select
        cast(null as bigint) as course_id
        , program_id
        , wagtail_page_id
        , cms_programpage_description as page_description
        , cms_programpage_duration as page_duration
        , cms_programpage_format as page_format
        , cms_programpage_time_commitment as page_time_commitment
        , cms_programpage_thumbnail_image_id as page_thumbnail_image_id
        , cms_programpage_min_weeks as page_min_weeks
        , cms_programpage_max_weeks as page_max_weeks
        , cms_programpage_min_weekly_hours as page_min_weekly_hours
        , cms_programpage_max_weekly_hours as page_max_weekly_hours
    from {{ ref('stg__mitxpro__app__postgres__cms_programpage') }}
)

-- ProductPage.certificate_page is the first live child page that is a CertificatePage
, certificate_pages as (
    select
        product_pages.wagtail_page_id
        , certificates.cms_certificate_ceus
        , row_number() over (
            partition by product_pages.wagtail_page_id order by certificate_wagtail.wagtail_page_path
        ) as certificate_rank
    from product_pages
    inner join pages as product_wagtail
        on product_pages.wagtail_page_id = product_wagtail.wagtail_page_id
    inner join pages as certificate_wagtail
        on
            certificate_wagtail.wagtail_page_path like {{ dbt.concat(["product_wagtail.wagtail_page_path", "'%'"]) }}
            and certificate_wagtail.wagtail_page_depth = product_wagtail.wagtail_page_depth + 1
            and certificate_wagtail.wagtail_page_is_live
    inner join {{ ref('stg__mitxpro__app__postgres__cms_certificatepage') }} as certificates
        on certificate_wagtail.wagtail_page_id = certificates.wagtail_page_id
)

-- ProductPage.faculty is the first live child page that is a FacultyMembersPage
, faculty_pages as (
    select
        product_pages.wagtail_page_id
        -- parenthesized because the macro's Trino form ends in a line comment
        , (
            {{ json_array_field_values('faculty.cms_facultymemberspage_faculty', 'value.name') }}
        ) as instructors
        , row_number() over (
            partition by product_pages.wagtail_page_id order by faculty_wagtail.wagtail_page_path
        ) as faculty_rank
    from product_pages
    inner join pages as product_wagtail
        on product_pages.wagtail_page_id = product_wagtail.wagtail_page_id
    inner join pages as faculty_wagtail
        on
            faculty_wagtail.wagtail_page_path like {{ dbt.concat(["product_wagtail.wagtail_page_path", "'%'"]) }}
            and faculty_wagtail.wagtail_page_depth = product_wagtail.wagtail_page_depth + 1
            and faculty_wagtail.wagtail_page_is_live
    inner join {{ ref('stg__mitxpro__app__postgres__cms_facultymemberspage') }} as faculty
        on faculty_wagtail.wagtail_page_id = faculty.wagtail_page_id
)

select
    product_pages.course_id
    , product_pages.program_id
    , product_pages.wagtail_page_id
    , pages.wagtail_page_is_live as page_is_live
    , pages.wagtail_page_first_published_on as page_first_published_on
    , pages.wagtail_page_last_published_on as page_last_published_on
    , product_pages.page_description
    , product_pages.page_duration
    , product_pages.page_format
    , product_pages.page_time_commitment
    , product_pages.page_min_weeks
    , product_pages.page_max_weeks
    , product_pages.page_min_weekly_hours
    , product_pages.page_max_weekly_hours
    , images.image_url as page_thumbnail_url
    , certificate_pages.cms_certificate_ceus as page_ceus
    , faculty_pages.instructors as page_instructors
from product_pages
inner join pages on product_pages.wagtail_page_id = pages.wagtail_page_id
left join images on product_pages.page_thumbnail_image_id = images.image_id
left join certificate_pages
    on
        product_pages.wagtail_page_id = certificate_pages.wagtail_page_id
        and certificate_pages.certificate_rank = 1
left join faculty_pages
    on product_pages.wagtail_page_id = faculty_pages.wagtail_page_id and faculty_pages.faculty_rank = 1
