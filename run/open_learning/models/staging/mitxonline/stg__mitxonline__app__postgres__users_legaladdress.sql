create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__users_legaladdress__dbt_tmp

as (
    -- MITx Online User Information

    with source as (
        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__mitxonline__app__postgres__users_legaladdress
    )

    , cleaned as (

        select
            id as user_address_id
            , country as user_address_country
            , user_id
            , first_name as user_first_name
            , last_name as user_last_name
        from source
    )

    select * from cleaned
);
