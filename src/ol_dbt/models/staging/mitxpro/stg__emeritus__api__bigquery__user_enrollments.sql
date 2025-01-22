with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__emeritus__bigquery__api_enrollments') }}
)

{{ deduplicate_query(cte_name1='source', cte_name2='most_recent_source'
, partition_columns = 'batch_id, email, first_name, last_name') }}

, cleaned as (
    select
        ---course run
        batch_id as courserun_id
        , batch as courserun_title
        , {{ cast_timestamp_to_iso8601('batch_start_date') }} as courserun_start_on
        , {{ cast_timestamp_to_iso8601('batch_end_date') }} as courserun_end_on
        --wrike_run_code maps to external_course_run_id in xPro app
        , wrike_run_code as courserun_external_readable_id

        ---user
        , student_id as user_id
        , first_name as user_first_name
        , last_name as user_last_name
        , company_name as user_company
        , work_exp_slab as user_years_experience
        , nationality as user_nationality
        , residential_zip_code as user_address_postal_code
        , country as user_address_country
        , gdpr_agree as user_gdpr_agree
        , {{ cast_timestamp_to_iso8601('gdpr_consent_date') }} as user_gdpr_consent_date

        ---enrollment
        , enrollment_type
        , {{ cast_timestamp_to_iso8601('opportunity_created_date') }} as enrollment_created_on
        , {{ cast_timestamp_to_iso8601('opportunity_last_modified_date') }} as enrollment_updated_on
        , if(lower(status) = 'enrolled', true, false) as is_enrolled
        , if("deferred" = true, 'deferred', null) as enrollment_status  -- noqa: ST10

        , if(email = '(blank)', null, email) as user_email
        , if("function" = 'No Response', null, "function") as user_job_function  -- noqa: ST10
        , if(job_title in ('No Response', '(Blank)'), null, job_title) as user_job_title
        , if(industry in ('No Response', '(blank)'), null, industry) as user_industry
        , if(residential_address = 'NO RESPONSE', null, residential_address) as user_address_street
        , if(residential_city = 'NO RESPONSE', null, residential_city) as user_address_city
        , if(residential_state = 'NO RESPONSE', null, residential_state) as user_address_state

        , concat(first_name, ' ', last_name) as user_full_name
        --- match these values to our existing values from other platforms
        , case
            when gender = 'Prefer not to answer' then 'Other/Prefer Not to Say'
            when gender = 'Nonbinary' then 'Non-binary/non-conforming'
            when gender = 'Prefer not to answer' then 'Other/Prefer Not to Say'
            when gender = 'No Response' then null
            else gender
        end as user_gender
    from most_recent_source
)

select * from cleaned
