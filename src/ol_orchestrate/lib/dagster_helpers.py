import re

from dagster._core.definitions.partition import INVALID_PARTITION_SUBSTRINGS


def sanitize_mapping_key(mapping_key: str, replacement: str = "__") -> str:
    return re.sub(r"[^A-Za-z0-9_]", replacement, mapping_key)


def contains_invalid_partition_strings(partition_key: str) -> bool:
    return any(substr in partition_key for substr in INVALID_PARTITION_SUBSTRINGS)
