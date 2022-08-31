with runs as (
    select
        id
        , title
        , end_date
        , start_date
        , courseware_id
        , 'n/a' as courseware_url_path
    from {{ ref('stg__mitxonline__app__postgres__courses_courserun') }}
)

select * from runs
