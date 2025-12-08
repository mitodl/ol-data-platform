# ol-orchestrate-lib

Shared library for MIT Open Learning Dagster orchestration projects.

This package provides common resources, IO managers, utilities, and helpers used across
multiple Dagster code locations in the MIT Open Learning data platform.

## Contents

- **lib/**: Utility functions, constants, and helpers
- **resources/**: Dagster resource definitions (API clients, databases, etc.)
- **io_managers/**: Custom IO managers for S3, GCS, etc.
- **partitions/**: Partition definitions

## Installation

```bash
pip install ol-orchestrate-lib
```

## Development

This package is part of the ol-data-platform monorepo and uses uv for dependency management.
