"""Unit tests for ol_orchestrate.lib.utils."""

from __future__ import annotations

import pytest
from ol_orchestrate.lib.utils import flatten_nested_dict


def test_flatten_nested_dict_joins_key_paths() -> None:
    nested = {"a": 1, "b": {"c": 2, "d": {"e": 3}}}

    assert flatten_nested_dict(nested, "__") == {"a": 1, "b__c": 2, "b__d__e": 3}


def test_flatten_nested_dict_uses_the_given_delimiter() -> None:
    assert flatten_nested_dict({"a": {"b": 1}}, ".") == {"a.b": 1}


def test_flatten_nested_dict_keeps_lists_as_values() -> None:
    nested = {"block": {"children": ["x", {"y": 1}], "fields": {"tags": []}}}

    assert flatten_nested_dict(nested, "__") == {
        "block__children": ["x", {"y": 1}],
        "block__fields__tags": [],
    }


def test_flatten_nested_dict_drops_empty_mappings() -> None:
    assert flatten_nested_dict({"a": {}, "b": {"c": {}}, "d": None}, "__") == {
        "d": None
    }


def test_flatten_nested_dict_rejects_colliding_key_paths() -> None:
    with pytest.raises(ValueError, match="duplicated key 'a__b'"):
        flatten_nested_dict({"a__b": 1, "a": {"b": 2}}, "__")
