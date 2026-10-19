-- The CMS page of each MITx Online course and program, with the fields MITx Online's catalog
-- API reads off it: the feature image URL as cms.api.get_wagtail_img_src builds it and the
-- names of the linked instructor pages. It also carries the rich-text sections MIT Learn reads
-- from the Wagtail pages API (/api/v2/pages/) for a resource's full description.

with pages as (
    select * from {{ ref('stg__mitxonline__app__postgres__cms_wagtail_page') }}
)

, images as (
    select
        image_id
        , {{ dbt.concat(["image_url", "'?v='", "image_file_hash"]) }} as image_src
    from {{ ref('stg__mitxonline__app__postgres__wagtailimages_image') }}
    where image_file is not null and image_file != ''
)

, product_pages as (
    select
        course_id
        , cast(null as bigint) as program_id
        , wagtail_page_id
        , course_description as page_description
        , course_about as page_about
        , course_what_you_learn as page_what_you_learn
        , course_prerequisites as page_prerequisites
        , course_length as page_length
        , course_effort as page_effort
        , course_feature_image_id as page_feature_image_id
        , course_min_weeks as page_min_weeks
        , course_max_weeks as page_max_weeks
        , course_min_weekly_hours as page_min_weekly_hours
        , course_max_weekly_hours as page_max_weekly_hours
        , course_min_price as page_min_price
        , course_max_price as page_max_price
        , course_is_in_learn_catalog as page_is_in_learn_catalog
        , course_ingests_content_files_for_ai as page_ingests_content_files_for_ai
    from {{ ref('stg__mitxonline__app__postgres__cms_coursepage') }}
    union all
    select
        cast(null as bigint) as course_id
        , program_id
        , wagtail_page_id
        , program_description as page_description
        , program_about as page_about
        , program_what_you_learn as page_what_you_learn
        , program_prerequisites as page_prerequisites
        , program_length as page_length
        , program_effort as page_effort
        , program_feature_image_id as page_feature_image_id
        , program_min_weeks as page_min_weeks
        , program_max_weeks as page_max_weeks
        , program_min_weekly_hours as page_min_weekly_hours
        , program_max_weekly_hours as page_max_weekly_hours
        , program_min_price as page_min_price
        , program_max_price as page_max_price
        , program_is_in_learn_catalog as page_is_in_learn_catalog
        , cast(null as boolean) as page_ingests_content_files_for_ai
    from {{ ref('stg__mitxonline__app__postgres__cms_programpage') }}
)

-- The API does not order the links (InstructorPageLink.order is unused), so the link id
-- stands in for the order it returns them in
, instructors as (
    select
        links.wagtail_page_id
        , array_agg(instructor_pages.instructor_name order by links.instructorpagelink_id) as instructors
    from {{ ref('stg__mitxonline__app__postgres__cms_instructorpagelink') }} as links
    inner join {{ ref('stg__mitxonline__app__postgres__cms_instructorpage') }} as instructor_pages
        on links.instructor_wagtail_page_id = instructor_pages.wagtail_page_id
    group by links.wagtail_page_id
)

select
    product_pages.course_id
    , product_pages.program_id
    , product_pages.wagtail_page_id
    , pages.wagtail_page_is_live as page_is_live
    , pages.wagtail_page_first_published_on as page_first_published_on
    , pages.wagtail_page_last_published_on as page_last_published_on
    , product_pages.page_description
    , product_pages.page_about
    , product_pages.page_what_you_learn
    , product_pages.page_prerequisites
    , product_pages.page_length
    , product_pages.page_effort
    , product_pages.page_min_weeks
    , product_pages.page_max_weeks
    , product_pages.page_min_weekly_hours
    , product_pages.page_max_weekly_hours
    , product_pages.page_min_price
    , product_pages.page_max_price
    , coalesce(product_pages.page_is_in_learn_catalog, false) as page_is_in_learn_catalog
    , coalesce(product_pages.page_ingests_content_files_for_ai, false) as page_ingests_content_files_for_ai
    , images.image_src as page_feature_image_src
    , instructors.instructors as page_instructors
from product_pages
inner join pages on product_pages.wagtail_page_id = pages.wagtail_page_id
left join images on product_pages.page_feature_image_id = images.image_id
left join instructors on product_pages.wagtail_page_id = instructors.wagtail_page_id
