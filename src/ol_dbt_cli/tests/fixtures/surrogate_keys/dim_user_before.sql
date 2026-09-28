-- Carved verbatim from src/ol_dbt/models/dimensional/dim_user.sql at a20a72865^ (before #2497).
--
-- Only the generate_surrogate_key call sites are kept: they are the entire
-- input to the drift detector, and the surrounding 800 lines of union branches
-- would not change what it reads. Do not reformat -- the point of the fixture
-- is that it is the text the incident actually shipped.

select
        {{ dbt_utils.generate_surrogate_key(['lower(email)']) }} as user_pk
        {{ dbt_utils.generate_surrogate_key(['lower(mitxpro_user_view.user_email)']) }} as user_pk
        {{ dbt_utils.generate_surrogate_key(['lower(user_email)']) }} as user_pk
        {{ dbt_utils.generate_surrogate_key(['lower(user_email)']) }} as user_pk
        {{ dbt_utils.generate_surrogate_key(['lower(mitxresidential_user_view.user_email)']) }} as user_pk
        {{ dbt_utils.generate_surrogate_key(['lower(bootcamps_user_view.user_email)']) }} as user_pk
