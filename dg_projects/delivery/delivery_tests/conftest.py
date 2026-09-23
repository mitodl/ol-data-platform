"""Keep importing ``delivery.definitions`` from hanging the suite.

``definitions.py`` calls ``authenticate_vault`` at module scope. With no cached
Vault token -- CI always, a laptop often -- that drops into the interactive OIDC
flow, which opens a browser and then blocks on a localhost callback socket. It
never raises, so the ``try/except`` around the call cannot catch it and the job
sits until the runner's six-hour timeout.

``VAULT_OIDC_NONINTERACTIVE`` turns that cache miss into an exception instead,
which the ``try/except`` does catch, falling back to ``unauthenticated_vault``.
Setting it here rather than in the one test that imports ``definitions`` means
the next test to import it does not have to rediscover this.
"""

import os

os.environ.setdefault("VAULT_OIDC_NONINTERACTIVE", "1")
