-- The tfact_enrollment post_hook must leave no Emeritus or Global Alumni row whose key is
-- gone from staging. Each such row counts one enrollment twice. Rows here mean the hook did
-- not run, or staging rebuilt after the fact did.
select enrollment_key
from {{ ref('tfact_enrollment') }}
where {{ tfact_enrollment_gone_from_source() }}
