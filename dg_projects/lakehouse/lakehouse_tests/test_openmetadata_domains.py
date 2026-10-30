"""Tests for the OpenMetadata domain rules applied to the uploaded manifest."""

from pathlib import Path
from typing import Any

import pytest
from lakehouse.lib.openmetadata_domains import (
    DomainRules,
    assign_domains,
    load_domain_rules,
)

RULES_PATH = Path(__file__).parents[3] / "src/ol_dbt/openmetadata_domains.yml"

RULES = DomainRules.model_validate(
    {
        "domains": ["learners-identity", "commerce"],
        "rules": [
            {"domain": None, "match": ["dim_date"]},
            {"domain": "commerce", "match": ["*order*"]},
            {"domain": "learners-identity", "match": ["*USER*"]},
        ],
    }
)


def _node(name: str, resource_type: str = "model", **meta: Any) -> dict[str, Any]:
    return {"name": name, "resource_type": resource_type, "meta": meta}


def _manifest(*nodes: dict[str, Any], sources: list[dict[str, Any]] | None = None):
    return {
        "nodes": {
            f"{node['resource_type']}.pkg.{node['name']}": node for node in nodes
        },
        "sources": {f"source.pkg.raw.{s['name']}": s for s in sources or []},
    }


def test_the_repo_rules_file_is_valid():
    rules = load_domain_rules(RULES_PATH)

    assert rules.rule_for("dim_user") is not None


@pytest.mark.parametrize(
    ("name", "domain"),
    [
        ("dim_concept", "courses-content"),
        ("snapshot_bridge_concept_relation", "courses-content"),
        ("integrations__learn__concepts", "courses-content"),
        ("courseware_claim", "courses-content"),
        ("int__courseware__claims", "courses-content"),
        # Open edX's courseware app holds learner state, not courseware knowledge.
        ("raw__mitx__openedx__mysql__courseware_studentmodule", "engagement"),
    ],
)
def test_the_repo_rules_place_courseware_names(name, domain):
    rule = load_domain_rules(RULES_PATH).rule_for(name)

    assert rule is not None
    assert rule.domain == domain


def test_first_matching_rule_wins_and_keeps_owner_and_tier():
    node = _node(
        "tfact_user_order", openmetadata={"owner": "data-platform", "tier": "T2"}
    )

    counts, unassigned = assign_domains(_manifest(node), RULES)

    assert node["meta"]["openmetadata"] == {
        "owner": "data-platform",
        "tier": "T2",
        "domain": "commerce",
    }
    assert counts == {"commerce": 1}
    assert unassigned == []


def test_globs_ignore_case():
    node = _node("dim_user")

    assign_domains(_manifest(node), RULES)

    assert node["meta"]["openmetadata"]["domain"] == "learners-identity"


def test_a_domain_set_in_dbt_config_is_kept():
    node = _node("dim_user", openmetadata={"domain": "commerce"})

    counts, _ = assign_domains(_manifest(node), RULES)

    assert node["meta"]["openmetadata"]["domain"] == "commerce"
    assert counts == {"commerce": 1}


def test_a_null_rule_and_no_rule_leave_meta_alone():
    reference = _node("dim_date")
    stray = _node("dim_widget")

    counts, unassigned = assign_domains(_manifest(reference, stray), RULES)

    assert reference["meta"] == {}
    assert stray["meta"] == {}
    assert counts == {"(none)": 1}
    assert unassigned == ["dim_widget"]


def test_sources_snapshots_and_seeds_are_assigned_but_tests_are_not():
    source = _node("raw__app__users_user", resource_type="source")
    snapshot = _node("snapshot_order", resource_type="snapshot")
    seed = _node("user_roles", resource_type="seed")
    test = _node("not_null_dim_user_user_pk", resource_type="test")

    assign_domains(_manifest(snapshot, seed, test, sources=[source]), RULES)

    assert source["meta"]["openmetadata"]["domain"] == "learners-identity"
    assert snapshot["meta"]["openmetadata"]["domain"] == "commerce"
    assert seed["meta"]["openmetadata"]["domain"] == "learners-identity"
    assert test["meta"] == {}


def test_a_rule_naming_an_unlisted_domain_is_rejected():
    with pytest.raises(ValueError, match="comerce"):
        DomainRules.model_validate(
            {
                "domains": ["commerce"],
                "rules": [{"domain": "comerce", "match": ["*order*"]}],
            }
        )


def test_the_uploaded_rules_keep_the_shape_the_domains_job_reads():
    # ol-infrastructure's domain_assignment.py reads each rule's domain and
    # match from this dump, in order.
    rules = load_domain_rules(RULES_PATH)

    assert [
        (rule["domain"], rule["match"]) for rule in rules.model_dump()["rules"]
    ] == [(rule.domain, rule.match) for rule in rules.rules]
