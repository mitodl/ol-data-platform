-- MITx Courses Information sourced from edx.org

-- Note that course id here is always in old format as {org}/{course number}/{run} as IR converts the new format
-- to old format in order to preserve functionality of old apps
-- e.g course_id: "MITx/14.73x/3T2020" is the same course run "course-v1:MITx+14.73x+3T2020" in MITxOnline

with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__irx__edxorg__bigquery__mitx_course') }}

)

, renamed as (

    select
        course_title as courserun_title
        , semester as courserun_semester
        , url as courserun_url
        , instructors as courserun_instructors
        , registration_open as courserun_enrollment_start_date
        , course_launch as courserun_start_date
        , course_wrap as courserun_end_date
        , case course_id
            when 'MITx/14.74x/3T2015' then '14.740x'
            when 'MITx/ESD.SCM1x/3T2014' then 'CTL.SC1x'
            when 'MITx/6.041x_3/2T2016' then '6.431x'
            when 'MITx/6.041x_4/1T2017' then '6.431x'
            else replace(course_number, ' ', '')
        end as course_number
        , case
            when institution = 'MITx_PRO' then 'MITxPRO'
            else institution
        end as courserun_institution
        , replace(course_id, 'ESD.SCM1x', 'CTL.SC1x') as courserun_readable_id
        ,{{ translate_course_id_to_platform('course_id') }} as courserun_platform
        , coalesce(self_paced = 1, false) as courserun_is_self_paced
    from source

)

select * from renamed
