with source as (
    select *
    from dev.main_raw.raw__irx__edxorg__bigquery__email_opt_in
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
            ---- only course_id starts with xPRO are from xPro open edx platform
            when course_id like 'xPRO%' then 'xPro'
            --- Some runs from course - VJx Visualizing Japan (1850s-1930s) that run on edx don't start with 'MITx/`
            --- e.g. VJx/VJx_S/3T2015, VJx/VJx/3T2014, VJx/VJx_2/3T2016
            when course_id like 'MITx/%' or course_id like 'VJx%' then 'edX.org'
        end
        as courserun_platform
        ,
        to_iso8601(from_iso8601_timestamp(preference_set_datetime))
        as user_email_opt_in_updated_on
    from source

)

select * from cleaned
