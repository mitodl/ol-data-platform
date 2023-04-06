

    create table ol_data_lake_qa.ol_warehouse_qa_staging.stg__bootcamps__app__postgres__courses_courserun__dbt_tmp
    WITH (format = 'PARQUET')
  as (
    -- Bootcamps Course Run Information

with source as (
    select * from ol_data_lake_qa.ol_warehouse_qa_raw.raw__bootcamps__app__postgres__klasses_bootcamprun
)

, cleaned as (
    select
        id as courserun_id
        , bootcamp_id as course_id
        , title as courserun_title
        , bootcamp_run_id as courserun_readable_id
        ,
    to_iso8601(from_iso8601_timestamp(start_date))
 as courserun_start_on
        ,
    to_iso8601(from_iso8601_timestamp(end_date))
 as courserun_end_on
    from source
)

select * from cleaned
  );
