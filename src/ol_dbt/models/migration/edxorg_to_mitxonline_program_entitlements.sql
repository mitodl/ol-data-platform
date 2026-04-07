with mitxonline_programs as (
    select * from {{ ref('int__mitxonline__programs') }}
)

, program_entitlements as (
    select
        case
          --- Handle titles such as Statistics and Data Science (General track) - RETIRED "old version"
          when program_title like 'Statistics and Data Science (General track)%'
              then 'Statistics and Data Science (General Track)'
           else program_title
        end as program_title
        , user_email
        , user_id
        , program_type
        , purchase_date
        , expiration_date
        , number_of_entitlements
        , number_of_redeemed_entitlements
    from {{ ref('stg__edxorg__program_entitlement') }}
)

, mitx_user as (
    select * from {{ ref('int__mitx__users') }}
)

, mitxonline_fulfilled_orders as (
    select * from {{ ref('int__mitxonline__ecommerce_order') }}
    where order_state = 'fulfilled'
)

, contenttypes as (
    select * from {{ ref('stg__mitxonline__app__postgres__django_contenttype') }}
)

, product_versions as (
    select * from {{ ref('stg__mitxonline__app__postgres__reversion_version') }}
    where
        contenttype_id in (
            select contenttypes.contenttype_id
            from
                contenttypes
            where contenttypes.contenttype_full_name = 'ecommerce_product'
        )
)

, program_contenttype as (
    select distinct contenttype_id
    from contenttypes
    where contenttype_full_name= 'courses_program'
)

, program_product as (
    select * from {{ ref('stg__mitxonline__app__postgres__ecommerce_product') }}
     where
        contenttype_id in (
            select contenttypes.contenttype_id
            from
                contenttypes
            where contenttypes.contenttype_full_name = 'courses_program'
        )
)

, program_product_versions as (
    select
        program_product.product_object_id
        , program_contenttype.contenttype_id
        , min(product_versions.version_id) as product_version_id
    from program_product
    inner join product_versions on program_product.product_id = product_versions.version_object_id
    cross join program_contenttype
    group by program_product.product_object_id, program_contenttype.contenttype_id
)

select
    mitxonline_programs.program_id
    , program_entitlements.program_title
    , program_entitlements.program_type
    , program_entitlements.purchase_date
    , program_entitlements.expiration_date
    , program_entitlements.number_of_entitlements
    , program_entitlements.number_of_redeemed_entitlements
    , mitxonline_fulfilled_orders.order_id
    , program_entitlements.user_email as user_edxorg_email
    , program_product_versions.product_object_id
    , program_product_versions.product_version_id
    , program_product_versions.contenttype_id
    , coalesce(mitx_user.user_mitxonline_id, mitx_user2.user_mitxonline_id) as user_mitxonline_id
from program_entitlements
left join mitxonline_programs
      on mitxonline_programs.program_title = program_entitlements.program_title
left join program_product_versions
      on mitxonline_programs.program_id = program_product_versions.product_object_id
left join mitxonline_fulfilled_orders
    on lower(program_entitlements.user_email) = lower(mitxonline_fulfilled_orders.user_email)
    and mitxonline_programs.program_readable_id = mitxonline_fulfilled_orders.program_readable_id
left join mitx_user
    on lower(program_entitlements.user_email) = lower(mitx_user.user_mitxonline_email)
left join mitx_user as mitx_user2
    on program_entitlements.user_id = mitx_user2.user_edxorg_id
