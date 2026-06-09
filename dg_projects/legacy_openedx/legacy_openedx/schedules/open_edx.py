"""Legacy Open edX pipeline schedule stubs.

Schedules for the legacy Open edX pipelines are defined in definitions.py
(alongside the jobs they target) so that Vault dynamic DB credentials are fetched
at schedule-evaluation time rather than at code-location load time.
"""
