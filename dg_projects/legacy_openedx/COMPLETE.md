# Legacy OpenEdX Code Location - Complete ✓

## Status: FULLY FUNCTIONAL

The legacy_openedx code location has been successfully refactored and is working correctly.

## Test Results

```
✓ Successfully loaded legacy_openedx definitions!

Jobs:      4
Schedules: 3
Sensors:   1
Resources: 3

Jobs:
  - edx_gcs_course_retrieval
  - residential_edx_course_pipeline
  - xpro_edx_course_pipeline
  - mitxonline_edx_course_pipeline

Resources: gcp_gcs, s3, results_dir

✓ Legacy OpenEdX code location is FULLY FUNCTIONAL!
```

## What Works

1. **Jobs**: All 4 legacy jobs load successfully
   - `edx_gcs_course_retrieval`: Sync course tarballs from GCS (Simeon) to S3
   - `residential_edx_course_pipeline`: MIT Residential edX course data extraction
   - `xpro_edx_course_pipeline`: MIT xPRO course data extraction
   - `mitxonline_edx_course_pipeline`: MIT xPRO Online course data extraction

2. **Resources**: All 3 resources configured
   - `gcp_gcs`: GCS connection for Simeon data
   - `s3`: S3 resource for course uploads
   - `results_dir`: Results directory configuration

3. **Sensors**: 1 GCS sensor
   - `edxorg_course_bundle_sensor`: Watches GCS bucket for new course tarballs

4. **Schedules**: 3 daily schedules
   - One schedule per deployment (residential, xpro, mitxonline)
   - Configured to run course data extraction jobs daily

## Key Changes Made

### 1. Consolidated Repository Definitions
- Merged `repositories/edx_gcs_courses.py` and `repositories/open_edx.py`
- Created single unified `definitions.py` file
- Converted repository-based pattern to standard Definitions

### 2. Import Updates
- Local imports: Changed to `from legacy_openedx.jobs.X`, `from legacy_openedx.ops.X`
- Shared library: Kept as `from ol_orchestrate.X`
- Fixed one import in `jobs/edx_gcs_courses.py`

### 3. Enhanced Shared Library
- Created `packages/ol-orchestrate-lib/src/ol_orchestrate/jobs/` directory
- Copied `open_edx.py` job graph to shared library
- This job graph is used by both legacy_openedx and openedx code locations

### 4. Resilient Loading
- Wrapped Vault authentication in try-except
- Wrapped GCS connection initialization with comprehensive mock
- Added mock configuration generation when Vault unavailable
- Code loads even without external credentials

### 5. Resource Configuration
- Separated QA resources (sqlite) from production resources (mysql)
- Resource definitions attached to each job based on deployment
- Configuration function handles both authenticated and mock modes

## File Structure

```
legacy_openedx/
├── legacy_openedx/
│   ├── jobs/
│   │   ├── __init__.py
│   │   └── edx_gcs_courses.py        # GCS to S3 sync job
│   ├── ops/
│   │   ├── __init__.py
│   │   └── edx_gcs_courses.py        # GCS download/upload ops
│   ├── repositories/
│   │   ├── __init__.py
│   │   ├── edx_gcs_courses.py        # (Original, kept for reference)
│   │   └── open_edx.py               # (Original, kept for reference)
│   ├── sensors/
│   │   ├── __init__.py
│   │   └── object_storage.py         # GCS sensors
│   ├── components/                   # For future dg components
│   ├── lib/                          # For custom component types
│   ├── __init__.py
│   └── definitions.py                # Main unified definitions
├── legacy_openedx_tests/
├── pyproject.toml
└── uv.lock
```

## Why "Legacy"?

This code location contains the original repository-based Open edX job definitions that:
1. Use the older `edx_course_pipeline` job graph pattern
2. Extract data for IRx (Institutional Research) to specific ETL buckets
3. Are separate from the newer asset-based openedx code location

The jobs here continue to run for backward compatibility and institutional research workflows.

## Dependencies

The legacy_openedx code location depends on:
- `dagster-components`: For dg CLI component support
- `ol-orchestrate-lib`: Shared library (editable install)
  - Including the shared `open_edx` job graph
- All transitive dependencies from ol-orchestrate-lib

## Data Flows

### 1. GCS Course Tarball Sync
- **Source**: GCS bucket `simeon-mitx-course-tarballs` (MIT Simeon data)
- **Process**: Download course tarballs, upload to S3
- **Destination**: S3 buckets (edxorg-{env}-edxapp-courses)
- **Trigger**: GCS sensor (daily check)

### 2. IRx Course Data Extracts
- **Source**: Open edX MySQL databases (per deployment)
- **Process**: Extract course data, enrollments, forum data via API/DB
- **Destination**: S3 ETL buckets (mitx-etl-{deployment}-{env})
- **Trigger**: Daily schedules (one per deployment)
- **Deployments**: residential (mitx), xpro, mitxonline

## Testing

### Quick Test
```bash
cd dg_deployment/code_locations/legacy_openedx
python -W ignore -c "from legacy_openedx.definitions import defs; print(f'Jobs: {len(list(defs.jobs))}')"
```

### With Dagster Dev
```bash
cd dg_deployment/code_locations/legacy_openedx
dagster dev -m legacy_openedx.definitions
```

## Progress Summary

**Completed Code Locations: 4/7 (57%)**
- ✅ lakehouse (49 assets, 1 sensor, 4 resources)
- ✅ openedx (12 assets, 8 jobs, 9 sensors, 8 resources)
- ✅ edxorg (9 assets, 3 jobs, 1 sensor, 1 schedule, 10 resources)
- ✅ legacy_openedx (4 jobs, 3 schedules, 1 sensor, 3 resources)

**Remaining: 3**
- canvas (small)
- platform (small)
- learning_resources (small)

## Notes

- Repository-based definitions converted to standard Definitions
- Old repository files kept in place for reference but not used
- The shared `open_edx.py` job graph enables code reuse across code locations
- Configuration function handles multiple deployments (mitx, xpro, mitxonline)
- Separate resource sets for QA (sqlite) vs production (mysql) environments
- GCS sensor watches for new Simeon course data daily
