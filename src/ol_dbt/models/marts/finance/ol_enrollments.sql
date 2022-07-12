with ol_enrollments as (

    select * from {{ ref('int_finance__ol_joined_enrollments') }}

)

select * from ol_enrollments 
order by org, created_on desc