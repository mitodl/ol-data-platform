{{ config(
    materialized='table',
    unique_key='time_key'
) }}

with minutes_of_day as (
    {{ dbt_utils.generate_series(1440) }}
)

-- integer-safe hour/minute derivations across engines.
, spine as (
    select cast(generated_number as integer) as n
    from minutes_of_day
)
, time_parts as (
    select
        n
        , n - 1 as minute_of_day
        , cast(floor((n - 1) / 60) as integer) as hour
        , (n - 1) % 60 as minute
    from spine
)
select
    -- integer HHMM, e.g. 0 → 0, 90 → 130, 870 → 1430
    (hour * 100) + minute as time_key
    , hour
    , minute
    , minute_of_day
    , case when hour < 12 then 'AM' else 'PM' end as am_pm
    , case
        when hour % 12 = 0 then 12
        else hour % 12
    end as hour_12
    , lpad(
        cast(
            case
                when hour % 12 = 0 then 12
                else hour % 12
            end as varchar
        ), 2, '0'
    )
    || ':'
    || lpad(cast(minute as varchar), 2, '0')
    || ' '
    || case when hour < 12 then 'AM' else 'PM' end
    as hour_of_day_label
    , case
        when hour between 0 and 5 then 'Night'
        when hour between 6 and 11 then 'Morning'
        when hour between 12 and 17 then 'Afternoon'
        else 'Evening'
    end as time_of_day_bucket
from time_parts
