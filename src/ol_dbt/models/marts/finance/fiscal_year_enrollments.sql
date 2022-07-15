with fiscal_year_enrollments as (

    select * from {{ ref('int_finance__joined_ol_enrollments') }}

)

select org, fiscal_year, count(user_id) as enrollment_count
from fiscal_year_enrollments 
group by org, fiscal_year
order by org, fiscal_year asc
