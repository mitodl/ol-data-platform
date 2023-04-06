

    create table ol_data_lake_qa.ol_warehouse_qa_tmacey_staging.stg__edxorg__bigquery__mitx_user_email_opt_in__dbt_tmp
    WITH (format = 'PARQUET')
  as (
    with source as (
    select *
    from ol_data_lake_qa.ol_warehouse_qa_tmacey.raw__irx__edxorg__bigquery__email_opt_in
)

, cleaned as (

    select
        user_id
        , email as user_email
        , username as user_username
        , full_name as user_full_name
        , is_opted_in_for_email as user_is_opted_in_for_email
        , course_id as courserun_readable_id
        ,
        case
            when course_id like 'MITxT%' then 'MITx Online'
            when course_id like 'xPRO%' or course_id like 'MITxPRO%' then 'xPro'
            --- Some runs from course - VJx Visualizing Japan (1850s-1930s) that run on edx don't start with 'MITx/`
            --- e.g. VJx/VJx_S/3T2015, VJx/VJx/3T2014, VJx/VJx_2/3T2016
            when course_id like 'MITx/%' or course_id like 'VJx%' then 'edX.org'
            else null
        end
 as courserun_platform
        ,
    to_iso8601(from_iso8601_timestamp(preference_set_datetime))
 as user_email_opt_in_updated_on
    from source

)

select * from cleaned
  );
