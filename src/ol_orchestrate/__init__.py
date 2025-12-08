"""
DEPRECATED: This module has been migrated to the new dg CLI structure.

The contents of this directory have been migrated as follows:

1. Shared library code (lib/, resources/, io_managers/, partitions/, jobs/, assets/openedx_course_archives.py):
   → packages/ol-orchestrate-lib/src/ol_orchestrate/

2. Code location-specific code:
   → dg_deployment/code_locations/{location_name}/{location_name}/

Code locations:
- lakehouse: DBT, Airbyte, Superset assets
- openedx: Open edX data extraction
- edxorg: edX.org data from IRx
- legacy_openedx: Legacy repository-based extracts
- canvas: Canvas LMS course exports
- orchestration_platform: Platform infrastructure & monitoring
- learning_resources: Learning platform API extraction

DO NOT import from this module. Use:
- from ol_orchestrate.lib import ...  (shared library)
- from {location_name}.assets import ...  (code location specific)

See: dg_deployment/code_locations/ for the new structure
See: packages/ol-orchestrate-lib/ for the shared library
"""  # noqa: E501

msg = (
    "This module has been deprecated and migrated to the dg CLI structure. "
    "See packages/ol-orchestrate-lib/ for shared code and "
    "dg_deployment/code_locations/ for code locations."
)
raise ImportError(msg)
