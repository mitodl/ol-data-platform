-- MicroMasters Program Information

with source as (
    select *
    from dev.main_raw.raw__micromasters__app__postgres__grades_micromastersprogramcertificate
)

, cleaned as (
    select
        id as programcertificate_id
        ,
        hash as programcertificate_hash
        ,
        program_id
        , user_id
        , to_iso8601(from_iso8601_timestamp(created_on))
        as programcertificate_created_on
        , to_iso8601(from_iso8601_timestamp(updated_on))
        as programcertificate_updated_on

    from source
)

select * from cleaned
