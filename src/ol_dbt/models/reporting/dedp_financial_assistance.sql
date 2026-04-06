with fp as (
    select * from {{ ref('int__mitxonline__flexiblepricing_flexiblepriceapplication') }}
)

, users as (
    select * from {{ ref('int__mitxonline__users') }}
)

, course_enrollments as (
    select * from {{ ref('int__micromasters__course_enrollments') }}
)

, runs as (
    select * from {{ ref('int__mitxonline__course_runs') }}
)

select
   distinct
   users.user_email
   , users.user_full_name
   , users.user_id
   , cast(fp.flexiblepriceapplication_income_usd as decimal(38,2)) as flexiblepriceapplication_income_usd
   , fp.flexiblepriceapplication_original_currency
   , cast(fp.flexiblepriceapplication_original_income as decimal(38,2)) as flexiblepriceapplication_original_income
   , fp.flexiblepriceapplication_status
   , fp.flexiblepriceapplication_exchange_rate_timestamp
   , fp.flexiblepriceapplication_created_on
   , fp.flexiblepriceapplication_updated_on
   , course_enrollments.courserunenrollment_is_active
   , course_enrollments.mitxonline_program_id
   , runs.courserun_start_on
   , runs.courserun_end_on
   , case when  fp.flexiblepriceapplication_income_usd < 10000 then true else false end as income_less_than_10000
   from fp
inner join users
    on fp.user_id = users.user_id
inner join course_enrollments
    on course_enrollments.user_id = fp.user_id
inner join runs
    on course_enrollments.courserun_readable_id = runs.courserun_readable_id
