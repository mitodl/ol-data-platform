with test_data as (
    select 'alice@mail.com' as user_mitxonline_email, null as user_edxorg_email
    union all
    select null, 'alice@mail.com'
    union all
    select 'bob@mail.com', 'bob@mail.com'
    union all
    select 'carol@mail.com', null
    union all
    select null, 'dan@mail.com'
)

select *
from {{ check_cross_column_duplicates('test_data', 'user_mitxonline_email', 'user_edxorg_email') }}
