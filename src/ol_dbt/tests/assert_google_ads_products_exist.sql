-- Products in google_ads_campaign_products with no course or program of that readable id.
select products.product_readable_id
from {{ ref('google_ads_campaign_products') }} as products
left join {{ ref('dim_course') }} as courses on products.product_readable_id = courses.course_readable_id
left join {{ ref('dim_program') }} as programs on products.product_readable_id = programs.program_readable_id
where courses.course_readable_id is null and programs.program_readable_id is null
