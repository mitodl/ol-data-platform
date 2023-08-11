--- This model combines intermediate users from different platforms,
-- this is built as a view with no additional data stored
{{ config(materialized='view') }}

with mitxonline_users as (
    select * from {{ ref('int__mitxonline__users') }}
)

, mitxpro_users as (
    select * from {{ ref('int__mitxpro__users') }}
)

, bootcamps_users as (
    select * from {{ ref('int__bootcamps__users') }}
)

, edxorg_users as (
    select * from {{ ref('int__edxorg__mitx_users') }}
)

, micromasters__users as (
    select * from {{ ref('int__micromasters__users') }}
)

, combined_users as (
    select
        '{{ var("mitxonline") }}' as platform
        , user_id
        , user_username
        , user_email
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
        , user_job_title
        , user_company
        , user_industry

    from mitxonline_users

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , user_id
        , user_username
        , user_email
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
        , user_job_title
        , user_company
        , user_industry

    from mitxpro_users

    union all

    select
        '{{ var("bootcamps") }}' as platform
        , user_id
        , user_username
        , user_email
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
        , user_job_title
        , user_company
        , user_industry

    from bootcamps_users

    union all

    select
        '{{ var("edxorg") }}' as platform
        , user_id
        , user_username
        , user_email
        , user_country as user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
        , null as user_job_title
        , null as user_company
        , null as user_industry

    from edxorg_users

    union all

    select  
        '{{ var("micromasters") }}' as platform
        , user_id
        , user_username
        , user_email
        , user_address_country
        , user_highest_education
        , user_gender
        , cast(substr(user_birth_date, 1, 4) as bigint) as user_birth_year
        , user_job_position as user_job_title
        , user_company_name as user_company
        , user_company_industry as user_industry

    from micromasters__users
)

select * from combined_users
