-- Carved verbatim from src/ol_dbt/models/dimensional/dim_user.sql at a20a72865 (#2497).
--
-- Only the generate_surrogate_key call sites are kept: they are the entire
-- input to the drift detector, and the surrounding 800 lines of union branches
-- would not change what it reads. Do not reformat -- the point of the fixture
-- is that it is the text the incident actually shipped.

select
        {{ dbt_utils.generate_surrogate_key([
            'user_identity_source',
            'user_identity_id'
        ]) }} as user_pk
