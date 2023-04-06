with linerunselection as (
    select *
    from dev.main_staging.stg__mitxpro__app__postgres__ecommerce_linerunselection
)

select
    linerunselection_id
    , line_id
    , courserun_id
    , linerunselection_created_on
    , linerunselection_updated_on
from linerunselection
