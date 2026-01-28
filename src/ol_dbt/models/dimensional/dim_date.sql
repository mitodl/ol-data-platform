{{ config(
    materialized='table',
    unique_key='date_key'
) }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2010-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
)

select
    cast(format_datetime(date_day, 'yyyyMMdd') as integer) as date_key
    , date_day as date
    , year(date_day) as year
    , quarter(date_day) as quarter
    , month(date_day) as month
    , day(date_day) as day
    , day_of_week(date_day) as day_of_week
    , format_datetime(date_day, 'EEEE') as day_name
    , week(date_day) as week_of_year
    , case when day_of_week(date_day) in (6, 7) then true else false end as is_weekend
    -- MIT Fiscal Year: July 1 - June 30
    , case
        when month(date_day) >= 7 then year(date_day) + 1
        else year(date_day)
      end as fiscal_year
    , case
        when month(date_day) >= 7 then quarter(date_day) - 2
        when month(date_day) <= 3 then quarter(date_day) + 2
        else quarter(date_day)
      end as fiscal_quarter
    -- MIT Academic Terms
    , case
        when month(date_day) in (9, 10, 11, 12) then 'Fall'
        when month(date_day) in (1) then 'IAP'
        when month(date_day) in (2, 3, 4, 5) then 'Spring'
        when month(date_day) in (6, 7, 8) then 'Summer'
      end as academic_term
    , case
        when month(date_day) in (9, 10, 11, 12) then year(date_day)
        else year(date_day) - 1
      end as academic_year
from date_spine
