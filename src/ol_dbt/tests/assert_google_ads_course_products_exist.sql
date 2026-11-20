-- product_readable_ids in google_ads_campaign_courses with no course or program of that id.
select seed.product_readable_id
from {{ ref('google_ads_campaign_courses') }} as seed
left join {{ ref('dim_course') }} as courses on seed.product_readable_id = courses.course_readable_id
left join {{ ref('dim_program') }} as programs on seed.product_readable_id = programs.program_readable_id
where
    seed.product_readable_id is not null
    and courses.course_readable_id is null
    and programs.program_readable_id is null
