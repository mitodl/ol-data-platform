with source as (
    select *
    from {{ source('ol_warehouse_raw_data', 'raw__bootcamps__app__postgres__applications_applicationstepsubmission') }}
)

{{ deduplicate_raw_table(order_by='id' , partition_columns = 'bootcamp_application_id, run_application_step_id') }}
, cleaned as (

    select
        id as submission_id
        , bootcamp_application_id as application_id
        , content_type_id as contenttype_id
        , object_id as submission_object_id
        , run_application_step_id as courserun_applicationstep_id
        , review_status as submission_review_status
        , submission_status
        , {{ cast_timestamp_to_iso8601('submitted_date') }} as submission_submitted_on
        , {{ cast_timestamp_to_iso8601('review_status_date') }} as submission_reviewed_on
        , {{ cast_timestamp_to_iso8601('created_on') }} as submission_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as submission_updated_on
    from most_recent_source
)

select * from cleaned
