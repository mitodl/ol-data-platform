-- The tfact_enrollment post_hook must leave no Emeritus or Global Alumni row whose key is
-- gone from staging. Up to 10 rows warn: the hook did not run, or staging rebuilt after the
-- fact did. More fail the build, because the hook skips a platform whose staging has shrunk,
-- and that needs a person to look at the load. Don't clear a failure with --full-refresh
-- before then: it rebuilds from staging, and raw may no longer hold the rows it would drop.
select enrollment_key
from {{ ref('tfact_enrollment') }}
where {{ tfact_enrollment_gone_from_source() }}
