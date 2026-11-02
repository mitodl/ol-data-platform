"""The frozen rule for deciding that two concept labels name the same concept.

Every agreement and recall number depends on this rule, so it is one function used
everywhere and reported with every figure. Two labels match when, in order:

1. their normalized keys are equal (case, punctuation and spacing ignored, the same
   normalization the courseware_label_key macro gives concept_label_key);
2. a resolution run assigned both keys to the same concept id;
3. an extra mapping (e.g. a resolution of both models' labels together, or a
   reviewer's label map for gold labels) sends them to the same canonical key.

Matches are transitive: the rule is the union of these equivalences.
"""

from collections.abc import Iterable

import polars as pl
from ml.lib.courseware.tagging import label_key

MATCHING_RULE = "normalized label, then resolved concept id, then supplied label map"


class LabelMatcher:
    """Union-find over label keys, concept ids and mapped canonical keys."""

    def __init__(self) -> None:
        """Start with every label in its own class."""
        self._parent: dict[str, str] = {}

    def _find(self, node: str) -> str:
        self._parent.setdefault(node, node)
        while self._parent[node] != node:
            self._parent[node] = self._parent[self._parent[node]]
            node = self._parent[node]
        return node

    def _union(self, left: str, right: str) -> None:
        left_root, right_root = self._find(left), self._find(right)
        if left_root != right_root:
            # the smaller root wins so the canonical id does not depend on load order
            low, high = sorted((left_root, right_root))
            self._parent[high] = low

    def add_assignments(self, assignments: pl.DataFrame) -> "LabelMatcher":
        """Join keys assigned to one concept (concept_label_key, concept_id)."""
        for key, concept_id in assignments.select(
            "concept_label_key", "concept_id"
        ).iter_rows():
            self._union(f"key:{key}", f"concept:{concept_id}")
        return self

    def add_mapping(self, pairs: Iterable[tuple[str, str]]) -> "LabelMatcher":
        """Join each label key to a canonical label key."""
        for key, canonical in pairs:
            self._union(f"key:{key}", f"key:{canonical}")
        return self

    def to_frame(self) -> pl.DataFrame:
        """Return every node with its class root, which from_frame restores."""
        nodes = sorted(self._parent)
        return pl.DataFrame(
            {"node": nodes, "root": [self._find(node) for node in nodes]},
            schema={"node": pl.String, "root": pl.String},
        )

    @classmethod
    def from_frame(cls, frame: pl.DataFrame) -> "LabelMatcher":
        """Rebuild a matcher from to_frame output."""
        matcher = cls()
        for node, root in frame.select("node", "root").iter_rows():
            matcher._parent[node] = root
            matcher._parent.setdefault(root, root)
        return matcher

    def canonical(self, label: str) -> str:
        """Return a label's match class; two labels match exactly when equal."""
        key = label_key(label)
        # a label with no key (only symbols) matches nothing but itself
        return self._find(f"key:{key}" if key else f"label:{label.strip()}")
