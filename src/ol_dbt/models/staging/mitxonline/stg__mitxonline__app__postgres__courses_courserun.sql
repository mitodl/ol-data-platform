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
        , if(has_courseware_url=true
           , concat('{{ var("mitxonline_openedx_url") }}', '/learn/course/', courseware_id,'/home')
            , null
        ) as courserun_url
        , run_tag as courserun_tag
        , is_self_paced as courserun_is_self_paced
        , b2b_contract_id
        , case
            when courseware_id like 'MITx/%' or courseware_id like 'course-v1:MITx+%'
                then '{{ var("edxorg") }}'
            else '{{ var("mitxonline") }}'
        end as courserun_platform
        , replace(replace(courseware_id, 'course-v1:', ''), '+', '/') as courserun_edx_readable_id
        ,{{ cast_timestamp_to_iso8601('start_date') }} as courserun_start_on
        ,{{ cast_timestamp_to_iso8601('end_date') }} as courserun_end_on
        ,{{ cast_timestamp_to_iso8601('enrollment_start') }} as courserun_enrollment_start_on
        ,{{ cast_timestamp_to_iso8601('enrollment_end') }} as courserun_enrollment_end_on
        ,{{ cast_timestamp_to_iso8601('expiration_date') }} as courserun_expired_on
        ,{{ cast_timestamp_to_iso8601('upgrade_deadline') }} as courserun_upgrade_deadline
        ,{{ cast_timestamp_to_iso8601('certificate_available_date') }} as courserun_certificate_available_on
        ,{{ cast_timestamp_to_iso8601('created_on') }} as courserun_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as courserun_updated_on
    from source
)

select * from cleaned
