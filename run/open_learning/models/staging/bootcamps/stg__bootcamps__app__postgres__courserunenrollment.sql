

    create table ol_data_lake_qa.ol_warehouse_qa_staging.stg__bootcamps__app__postgres__courserunenrollment__dbt_tmp
    WITH (format = 'PARQUET')
  as (
    -- Bootcamps Course Run Enrollment Information

with source as (
    select * from ol_data_lake_qa.ol_warehouse_qa_raw.raw__bootcamps__app__postgres__klasses_bootcamprunenrollment
)

, renamed as (
    select
        id as courserunenrollment_id
        , active as courserunenrollment_is_active
        , user_id
        , bootcamp_run_id as courserun_id
        , change_status as courserunenrollment_enrollment_status
        , user_certificate_is_blocked as courserunenrollment_is_certificate_blocked
        ,
    to_iso8601(from_iso8601_timestamp(created_on))
 as courserunenrollment_created_on
        ,
    to_iso8601(from_iso8601_timestamp(updated_on))
 as courserunenrollment_updated_on
        ,
    to_iso8601(from_iso8601_timestamp(novoed_sync_date))
 as courserunenrollment_novoed_sync_on
    from source
)

select * from renamed
  );
