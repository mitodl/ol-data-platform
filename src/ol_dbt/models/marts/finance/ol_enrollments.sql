with ol_enrollments as (

    select * from {{ ref('int_finance__joined_ol_enrollments') }}

)

select * from ol_enrollments 
order by org, created_on desc
