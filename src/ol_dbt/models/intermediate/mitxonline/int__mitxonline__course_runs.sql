with runs as (
    select
        courserun_id
        , courserun_title
        , courserun_end_on
        , courserun_start_on
        , courserun_readable_id
        , courserun_url
    from {{ ref('stg__mitxonline__app__postgres__courses_courserun') }}
)

select * from runs
