-- MITx Online Course Run Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_courserun') }}
)

, cleaned as (
    select
        id as courserun_id
        , course_id
        , live as courserun_is_live
        , title as courserun_title
        , courseware_id as courserun_readable_id
        , courseware_url_path as courserun_url
        , run_tag as courserun_tag
        , is_self_paced as courserun_is_self_paced
        , replace(replace(courseware_id, 'course-v1:', ''), '+', '/') as courserun_edx_readable_id
        , {{ cast_timestamp_to_iso8601('start_date') }} as courserun_start_on
        , {{ cast_timestamp_to_iso8601('end_date') }} as courserun_end_on
        , {{ cast_timestamp_to_iso8601('enrollment_start') }} as courserun_enrollment_start_on
        , {{ cast_timestamp_to_iso8601('enrollment_end') }} as courserun_enrollment_end_on
        , {{ cast_timestamp_to_iso8601('expiration_date') }} as courserun_expired_on
        , {{ cast_timestamp_to_iso8601('upgrade_deadline') }} as courserun_upgrade_deadline
        , {{ cast_timestamp_to_iso8601('created_on') }} as courserun_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as courserun_updated_on
    from source
)

select * from cleaned
