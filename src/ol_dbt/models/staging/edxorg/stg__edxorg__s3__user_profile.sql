with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__edxorg__s3__tables__auth_userprofile") }})

    {{ deduplicate_raw_table(order_by="_airbyte_extracted_at", partition_columns="id") }},
    cleaned as (
        select
            -- -all values are ingested as string, so we need to cast here to match other data sources
            cast(id as integer) as user_profile_id,
            cast(user_id as integer) as user_id,
            meta as user_profile_metadata,
            case
                when lower(name) = 'null' then null when name = '' then null when name like '><%' then null else name
            end as user_full_name,
            case when lower(bio) = 'null' then null when bio = '' then null else bio end as user_bio,
            case
                when lower(goals) = 'null'
                then null
                when goals = ''
                then null
                when goals like '><%'
                then null
                else goals
            end as user_goals,
            case
                when lower(year_of_birth) = 'null'
                then null
                when year_of_birth = ''
                then null
                else try_cast(year_of_birth as integer)
            end as user_year_of_birth,
            case
                when lower(country) = 'null' then null when country = '' then null else country
            end as user_address_country,
            {{ transform_gender_value("gender") }} as user_gender,
            {{ transform_education_value("level_of_education") }} as user_highest_education
        from most_recent_source
    )

select *
from cleaned
