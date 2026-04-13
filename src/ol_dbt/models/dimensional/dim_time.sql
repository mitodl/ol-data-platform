{{ config(
    materialized='table',
    unique_key='time_key'
) }}

with minutes_of_day as (
    {{ dbt_utils.generate_series(1440) }}
)

-- power(2, n) in generate_series returns DOUBLE; cast to INTEGER to ensure
-- integer division throughout and avoid fractional time_key values.
, spine as (
    select cast(generated_number as integer) as n
    from minutes_of_day
)

select
    -- integer HHMM, e.g. 0 → 0, 90 → 130, 870 → 1430
    ((n - 1) / 60) * 100 + ((n - 1) % 60) as time_key
    , (n - 1) / 60 as hour
    , (n - 1) % 60 as minute
    , n - 1 as minute_of_day
    , case when (n - 1) / 60 < 12 then 'AM' else 'PM' end as am_pm
    , case
        when (n - 1) / 60 % 12 = 0 then 12
        else (n - 1) / 60 % 12
    end as hour_12
    , lpad(
        cast(
            case
                when (n - 1) / 60 % 12 = 0 then 12
                else (n - 1) / 60 % 12
            end as varchar
        ), 2, '0'
    )
    || ':'
    || lpad(cast((n - 1) % 60 as varchar), 2, '0')
    || ' '
    || case when (n - 1) / 60 < 12 then 'AM' else 'PM' end
    as hour_of_day_label
    , case
        when (n - 1) / 60 between 0 and 5 then 'Night'
        when (n - 1) / 60 between 6 and 11 then 'Morning'
        when (n - 1) / 60 between 12 and 17 then 'Afternoon'
        else 'Evening'
    end as time_of_day_bucket
from spine
