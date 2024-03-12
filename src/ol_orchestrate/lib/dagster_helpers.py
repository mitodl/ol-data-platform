import re


def sanitize_mapping_key(mapping_key: str, replacement: str = "__") -> str:
    return re.sub(r"[^A-Za-z0-9_]", replacement, mapping_key)
