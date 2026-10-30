"""JSON values kept as the text a raw column holds.

Sources that land JSON Lines files keep every field as text and let staging cast
what it needs. A nested value becomes its JSON text, because dlt would otherwise
flatten each key of an object into a column of its own and each list into a
child table.
"""

import json
from typing import Any


def as_text(value: Any) -> str | None:  # noqa: ANN401
    """Return ``value`` as text, JSON-encoding anything that is not a string.

    :param value: One field of a parsed JSON line.
    :returns: The string itself, None for null, else the value's JSON text.
    :rtype: str | None
    """
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value)
