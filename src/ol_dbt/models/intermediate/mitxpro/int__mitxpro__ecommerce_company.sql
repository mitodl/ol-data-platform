with company as (select * from {{ ref("stg__mitxpro__app__postgres__ecommerce_company") }})

select company_id, company_name, company_updated_on, company_created_on
from company
