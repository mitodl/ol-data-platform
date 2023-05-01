-- MicroMasters Course Run metadata Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__micromasters__app__postgres__courses_courserun') }}
)

, cleaned as (
    select
        id as courserun_id
        , course_id
        , title as courserun_title
        , edx_course_key as courserun_readable_id
        , replace(replace(edx_course_key, 'course-v1:', ''), '+', '/') as courserun_edxorg_readable_id
        , case
            when edx_course_key like 'MITx/%' then split(edx_course_key, '/')[3]
            when edx_course_key like 'course-v1:%' then split(edx_course_key, '+')[3]
        end as courserun_tag
        , case
            when courseware_backend = 'mitxonline' then '{{ var("mitxonline") }}'
            when courseware_backend = 'edxorg' then '{{ var("edxorg") }}'
            else courseware_backend
        end as courserun_platform
        , enrollment_url as courserun_enrollment_url
        , is_discontinued as courserun_is_discontinued
        , prerequisites as courserun_prerequisites
        ,{{ cast_timestamp_to_iso8601('start_date') }} as courserun_start_on
        ,{{ cast_timestamp_to_iso8601('end_date') }} as courserun_end_on
        ,{{ cast_timestamp_to_iso8601('enrollment_start') }} as courserun_enrollment_start_on
        ,{{ cast_timestamp_to_iso8601('enrollment_end') }} as courserun_enrollment_end_on
        ,{{ cast_timestamp_to_iso8601('freeze_grade_date') }} as courserun_grade_freeze_on
        ,{{ cast_timestamp_to_iso8601('upgrade_deadline') }} as courserun_upgrade_deadline
    from source
)

select * from cleaned
