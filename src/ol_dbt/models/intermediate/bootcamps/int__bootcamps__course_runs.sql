with runs as (
    select
        id
        , title
        , end_date
        , start_date
        , courseware_id
        , 'n/a' as courseware_url_path
    from {{ ref('stg__bootcamps__app__postgres__klasses_bootcamprun') }}
)

select * from runs
