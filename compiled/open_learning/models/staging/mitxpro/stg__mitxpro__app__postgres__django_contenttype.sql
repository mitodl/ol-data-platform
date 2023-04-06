with source as (

    select * from dev.main_raw.raw__xpro__app__postgres__django_content_type

)

, renamed as (

    select
        id as contenttype_id
        , concat_ws(
            '_'
            , app_label
            , model
        ) as contenttype_full_name
    from source

)

select * from renamed
