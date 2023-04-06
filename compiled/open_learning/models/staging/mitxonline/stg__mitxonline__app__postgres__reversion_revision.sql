with source as (

    select * from dev.main_raw.raw__mitxonline__app__postgres__reversion_revision

)

, renamed as (

    select
        id as revision_id
        , comment as revision_comment
        , user_id
        ,
        to_iso8601(from_iso8601_timestamp(date_created))
        as revision_date_created

    from source

)

select * from renamed
