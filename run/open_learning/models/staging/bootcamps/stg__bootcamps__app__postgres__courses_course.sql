

    create table ol_data_lake_qa.ol_warehouse_qa_staging.stg__bootcamps__app__postgres__courses_course__dbt_tmp
    WITH (format = 'PARQUET')
  as (
    -- Bootcamps Course Information

with source as (
    select * from ol_data_lake_qa.ol_warehouse_qa_raw.raw__bootcamps__app__postgres__klasses_bootcamp
)

, cleaned as (
    select
        id as course_id
        , title as course_title
    from source
)

select * from cleaned
  );
