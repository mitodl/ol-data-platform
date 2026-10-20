{{ config(error_if='>0') }}

-- local_timestamp_to_timestamptz is NULL where the timestamp or the zone is NULL, as
-- Trino's with_timezone is. Trino does not check the zone of a NULL timestamp, so an
-- unknown zone with a NULL timestamp is NULL rather than an error. The query fails
-- outright where an engine raises. Returns the cases whose result is wrong.
with cases as (
    select *
    from (
        values
        (cast(null as varchar), 'Bogus/Zone', cast(null as varchar))
        , (cast(null as varchar), cast(null as varchar), cast(null as varchar))
        , ('2024-06-01 12:00:00', cast(null as varchar), cast(null as varchar))
        , ('2024-06-01 12:00:00', 'America/New_York', '2024-06-01T16:00:00.000Z')
    ) as cases (local_time, zone_name, expected)
)

, results as (
    select
        local_time
        , zone_name
        , expected
        , {{ format_timestamp_as_iso8601(
            timestamptz_at_utc(local_timestamp_to_timestamptz('local_time', 'zone_name')), fractional_digits=3
        ) }} as actual
    from cases
)

select *
from results
where coalesce(actual, '<null>') != coalesce(expected, '<null>')
