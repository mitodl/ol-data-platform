with all_values as (

    select
        user_gender as value_field
        , count(*) as n_records

    from dev.main_staging.stg__edxorg__bigquery__mitx_user_info_combo
    group by user_gender

)

select *
from all_values
where value_field not in (
    'Male', 'Female', 'Other/Prefer Not to Say', '', 'None'
)
