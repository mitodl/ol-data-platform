import re


def sanitize_mapping_key(mapping_key: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "", mapping_key)
