"""Keep importing ``data_loading.definitions`` from hanging the suite.

``data_loading.defs.canvas.resources`` calls ``authenticate_vault`` at module
scope, so loading the code location does. With no cached Vault token -- CI
always, a laptop often -- that drops into the interactive OIDC flow, which
blocks on a localhost callback socket and never raises, so the ``try/except``
around the call cannot catch it. ``VAULT_OIDC_NONINTERACTIVE`` turns the cache
miss into an exception the ``try/except`` does catch. Same as
``delivery_tests/conftest.py``.
"""

import os

os.environ.setdefault("VAULT_OIDC_NONINTERACTIVE", "1")
