with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__mitx__openedx__mysql__user_api_userorgtag') }}

)

, renamed as (

    select
        org
        , modified
        , id
        , created
        , user_id
        , key
        , value

    from source

)

select * from renamed
