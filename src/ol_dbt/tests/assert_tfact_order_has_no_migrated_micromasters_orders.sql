-- The tfact_order post_hook must leave no MicroMasters order that MITx Online also holds,
-- or the order is counted twice. Any row means the hook did not run, so this fails at
-- one row, not the default ten.
{{ config(error_if='>0') }}

select order_fact.order_key
from {{ ref('tfact_order') }} as order_fact
inner join {{ ref('int__micromasters__orders') }} as micromasters_orders
    on order_fact.order_id = micromasters_orders.order_id
where
    order_fact.platform = 'micromasters'
    and micromasters_orders.order_is_migrated_to_mitxonline
