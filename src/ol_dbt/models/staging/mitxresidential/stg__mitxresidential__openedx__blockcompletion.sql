{{
    config(
        materialized="incremental",
        unique_key="blockcompletion_id",
        incremental_strategy="delete+insert",
        views_enabled=false,
    )
}}

with
    source as (
        select *
        from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__completion_blockcompletion") }}

        {% if is_incremental() %}
            where created >= (select max(this.blockcompletion_created_on) from {{ this }} as this)
        {% endif %}
    )

    -- - this is needed for the initial dbt run to deduplicate the data from raw table
    {{ deduplicate_raw_table(order_by="_airbyte_extracted_at", partition_columns="id") }},
    cleaned as (

        select
            id as blockcompletion_id,
            user_id,
            created as blockcompletion_created_on,
            modified as blockcompletion_updated_on,
            block_key as block_fk,
            block_type as block_category,
            completion as block_completed,
            course_key as courserun_readable_id
        from most_recent_source
    )

select *
from cleaned
