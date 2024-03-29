with source as (
    select *
    from {{ source('ol_warehouse_raw_data', 'raw__mitxonline__app__postgres__flexiblepricing_flexiblepricetier') }}
)

, renamed as (
    select
        source.id as flexiblepricetier_id
        , source."current" as flexiblepricetier_is_current
        , source.discount_id
        , source.courseware_object_id
        , source.income_threshold_usd as flexiblepricetier_income_threshold_usd
        , source.courseware_content_type_id as contenttype_id
        ,{{ cast_timestamp_to_iso8601('source.created_on') }} as flexiblepricetier_created_on
        ,{{ cast_timestamp_to_iso8601('source.updated_on') }} as flexiblepricetier_updated_on

    from source
)

select * from renamed
