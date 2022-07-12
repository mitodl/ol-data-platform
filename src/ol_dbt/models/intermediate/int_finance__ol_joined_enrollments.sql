-- int_finance__ol_joined_enrollments.sql

with mitxonline as (

    select *, 'mitxonline' as org from {{ ref('stg_mitxonline__app__postgres__course_courserunenrollment') }}

),

xpro as (

    select *, 'xpro' as org from {{ ref('stg_mitxpro__app_postgres__course_courserunenrollment') }}

),

join_ol_enrollments as (

    select * from mitxonline

    union all

    select * from xpro

)

select * from join_ol_enrollments