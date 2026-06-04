-- Bootcamps Course Run Information

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__bootcamps__app__postgres__klasses_bootcamprun') }}
)

, cleaned as (
    select
        id as courserun_id
        , bootcamp_id as course_id
        , title as courserun_title
        -- Manual backfill for runs predating the bootcamp_run_id field being populated
        , coalesce(
            bootcamp_run_id
            , case id
                when 2 then 'bootcamp-v1:public+culture-f2f+R1'
                when 4 then 'bootcamp-v1:public+sustainability-f2f+R1'
                when 11 then 'bootcamp-v1:public+DT-f2f+R1'
                when 14 then 'bootcamp-v1:public+HI-f2f+R3'
                when 16 then 'bootcamp-v1:public+IE-f2f+R10'
                when 17 then 'bootcamp-v1:public+DT-f2f+R2'
                when 18 then 'bootcamp-v1:public+FC-f2f+R1'
                when 19 then 'bootcamp-v1:public+design-f2f+R1'
         end) as courserun_readable_id
        ,{{ cast_timestamp_to_iso8601('start_date') }} as courserun_start_on
        ,{{ cast_timestamp_to_iso8601('end_date') }} as courserun_end_on
    from source
)

select * from cleaned
