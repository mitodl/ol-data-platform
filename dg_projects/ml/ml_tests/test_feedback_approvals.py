"""Tests for the feedback_category_approval batch-approval asset."""

import polars as pl
from dagster import IOManager, materialize
from ml.assets.feedback_approvals import (
    CategoryApprovalConfig,
    feedback_category_approval,
)


class _CapturingIOManager(IOManager):
    """Records handled outputs by name; enough to assert what got written."""

    def __init__(self) -> None:
        self.written: dict[str, pl.DataFrame] = {}

    def handle_output(self, context, obj) -> None:
        self.written[context.name] = obj

    def load_input(self, context) -> None:
        raise NotImplementedError


def test_batch_decisions_write_one_row_each_with_shared_approver() -> None:
    io_manager = _CapturingIOManager()
    config = CategoryApprovalConfig(
        approved_by="rlougee",
        decisions=[
            {"category_slug": "billing-refund", "category_status": "approved"},
            {"category_slug": "login-issue", "category_status": "approved"},
            {"category_slug": "junk-tag", "category_status": "deprecated"},
        ],
    )

    result = materialize(
        [feedback_category_approval],
        resources={"io_manager": io_manager},
        run_config={
            "ops": {
                "intermediate__feedback_category_approval": {
                    "config": config.model_dump()
                }
            }
        },
        raise_on_error=False,
    )

    assert result.success is True
    rows = io_manager.written["result"].to_dicts()
    assert [r["category_slug"] for r in rows] == [
        "billing-refund",
        "login-issue",
        "junk-tag",
    ]
    assert all(r["approved_by"] == "rlougee" for r in rows)
    # Same review pass, one timestamp -- not a per-row clock read.
    assert len({r["approved_at"] for r in rows}) == 1


def test_empty_decisions_fails() -> None:
    result = materialize(
        [feedback_category_approval],
        run_config={
            "ops": {
                "intermediate__feedback_category_approval": {
                    "config": CategoryApprovalConfig(
                        approved_by="rlougee", decisions=[]
                    ).model_dump()
                }
            }
        },
        raise_on_error=False,
    )

    assert result.success is False


def test_invalid_category_status_fails() -> None:
    result = materialize(
        [feedback_category_approval],
        run_config={
            "ops": {
                "intermediate__feedback_category_approval": {
                    "config": CategoryApprovalConfig(
                        approved_by="rlougee",
                        decisions=[{"category_slug": "a", "category_status": "bogus"}],
                    ).model_dump()
                }
            }
        },
        raise_on_error=False,
    )

    assert result.success is False
