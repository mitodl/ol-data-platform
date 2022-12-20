with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__django_content_type') }}

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
