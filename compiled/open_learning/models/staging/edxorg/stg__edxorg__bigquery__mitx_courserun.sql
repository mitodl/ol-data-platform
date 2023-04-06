-- MITx Courses Information sourced from edx.org

-- Note that course id here is always in old format as {org}/{course number}/{run} as IR converts the new format
-- to old format in order to preserve functionality of old apps
-- e.g course_id: "MITx/14.73x/3T2020" is the same course run "course-v1:MITx+14.73x+3T2020" in MITxOnline

with source as (

    select * from dev.main_raw.raw__irx__edxorg__bigquery__mitx_course

)

, renamed as (

    select
        course_number
        , course_title as courserun_title
        , semester as courserun_semester
        , url as courserun_url
        , instructors as courserun_instructors
        , registration_open as courserun_enrollment_start_date
        , course_launch as courserun_start_date
        , course_wrap as courserun_end_date
        , case
            when institution = 'MITx_PRO' then 'MITxPRO'
            else institution
        end as courserun_institution
        , replace(course_id, 'ESD.SCM1x', 'CTL.SC1x') as courserun_readable_id
        ,
        case
            when course_id like 'MITxT%' then 'MITx Online'
            ---- only course_id starts with xPRO are from xPro open edx platform
            when course_id like 'xPRO%' then 'xPro'
            --- Some runs from course - VJx Visualizing Japan (1850s-1930s) that run on edx don't start with 'MITx/`
            --- e.g. VJx/VJx_S/3T2015, VJx/VJx/3T2014, VJx/VJx_2/3T2016
            when course_id like 'MITx/%' or course_id like 'VJx%' then 'edX.org'
        end
        as courserun_platform
        , coalesce(self_paced = 1, false) as courserun_is_self_paced
    from source

)

select * from renamed
