with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__global_alumni__bigquery__api_enrollments") }})

    -- The raw table is loaded using full refresh + appends sync mode.
    -- As a result, every sync will create duplicates of already existing data,
    -- so we need to deduplicate the data.
    {{ deduplicate_raw_table(order_by="_airbyte_generation_id", partition_columns="batch_id, email") }},
    cleaned as (
        select
            -- -course run
            batch_id as courserun_id,
            batch as courserun_title,
            {{ cast_timestamp_to_iso8601("batch_start_date") }} as courserun_start_on,
            {{ cast_timestamp_to_iso8601("batch_end_date") }} as courserun_end_on,
            wrike_run_code as courserun_external_readable_id,
            -- -user
            student_id as user_id,
            first_name as user_first_name,
            last_name as user_last_name,
            email as user_email,
            company_name as user_company,
            work_exp_slab as user_years_experience,
            residential_zip_code as user_address_postal_code,
            country as user_address_country,
            gdpr_agree as user_gdpr_agree,
            if(residential_address = 'NO RESPONSE', null, residential_address) as user_address_street,
            if(residential_city = 'NO RESPONSE', null, residential_city) as user_address_city,
            if(residential_state = 'NO RESPONSE', null, residential_state) as user_address_state,
            if("function" = 'No Response', null, "function") as user_job_function,  -- noqa: ST10
            if(job_title = 'No Response', null, job_title) as user_job_title,
            if(industry = 'No Response', null, industry) as user_industry,
            {{ cast_timestamp_to_iso8601("gdpr_consent_date") }} as user_gdpr_consent_date,
            concat(first_name, ' ', last_name) as user_full_name,
            -- - match these values to our existing values from other platforms
            case
                when gender = 'Nonbinary'
                then 'Non-binary/non-conforming'
                when gender = 'No Response'
                then null
                else gender
            end as user_gender,
            -- -enrollment
            if(lower(status) = 'enrolled', true, false) as is_enrolled,
            if("deferred" = true, 'deferred', null) as enrollment_status  -- noqa: ST10
        from most_recent_source
    )

select *
from cleaned
