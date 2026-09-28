-- Carved verbatim from src/ol_dbt/models/dimensional/dim_discount.sql at e2c87bd3f^ (before #2411).
--
-- Only the generate_surrogate_key call sites are kept: they are the entire
-- input to the drift detector, and the surrounding 800 lines of union branches
-- would not change what it reads. Do not reformat -- the point of the fixture
-- is that it is the text the incident actually shipped.

select
    {{ dbt_utils.generate_surrogate_key(['cast(source_discount_id as varchar)', 'discount_code', 'platform_code']) }} as discount_pk
