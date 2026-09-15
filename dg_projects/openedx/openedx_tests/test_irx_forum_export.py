"""Tests for forum/contents.bson: the Mongo document shape and the dump format."""

import hashlib
from datetime import UTC, datetime
from typing import Any

import bson
import polars as pl
from bson import ObjectId
from openedx.assets.irx_export import forum_document, mint_objectid, write_forum_bson
from upath import UPath

THREAD_MONGOID = "55f06d88ef584fb23400000b"  # pragma: allowlist secret
RESPONSE_MONGOID = "55f0a034ef584f088600000c"  # pragma: allowlist secret
COMMENT_MONGOID = "55f0a034ef584f088600000d"  # pragma: allowlist secret
ENDORSEMENT = '{"user_id": "23372", "time": "2025-12-12 16:07:00.853844+00:00"}'

BLANK_ROW: dict[str, Any] = dict.fromkeys(
    (
        "_type",
        "id",
        "mongoid",
        "comment_thread_id",
        "comment_thread_mongoid",
        "parent_id",
        "parent_mongoid",
        "course_id",
        "author_id",
        "author_username",
        "title",
        "body",
        "thread_type",
        "context",
        "commentable_id",
        "group_id",
        "closed",
        "pinned",
        "endorsed",
        "endorsement",
        "visible",
        "anonymous",
        "anonymous_to_peers",
        "depth",
        "child_count",
        "comment_count",
        "created_at",
        "updated_at",
        "last_activity_at",
        "votes_up",
        "votes_down",
        "abuse_flaggers",
        "historical_abuse_flaggers",
    )
)
# Naive, as the Iceberg timestamps are.
CREATED = datetime.fromisoformat("2025-12-12 16:07:00.853844")


def _comment(**values: Any) -> dict[str, Any]:
    return {
        **BLANK_ROW,
        "_type": "Comment",
        "id": 42,
        "comment_thread_id": 7,
        "course_id": "course-v1:MITx+7.06r+2015_Fall",
        "author_id": 11154,
        "body": "Answer",
        "endorsed": False,
        "endorsement": "{}",
        "visible": True,
        "anonymous": False,
        "anonymous_to_peers": False,
        "depth": 0,
        "child_count": 0,
        "created_at": CREATED,
        "updated_at": CREATED,
        **values,
    }


def test_minted_ids_open_with_a_zero_timestamp_and_keep_kinds_apart() -> None:
    thread, comment = mint_objectid("CommentThread", 5), mint_objectid("Comment", 5)

    assert thread != comment
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    assert thread.generation_time == comment.generation_time == epoch


def test_a_migrated_comment_points_at_its_parents_by_objectid() -> None:
    document = forum_document(
        _comment(
            mongoid=COMMENT_MONGOID,
            comment_thread_mongoid=THREAD_MONGOID,
            parent_id=41,
            parent_mongoid=RESPONSE_MONGOID,
            depth=1,
            votes_up=["1464"],
            endorsed=True,
            endorsement=ENDORSEMENT,
        )
    )

    assert document["_id"] == ObjectId(COMMENT_MONGOID)
    assert document["comment_thread_id"] == ObjectId(THREAD_MONGOID)
    assert document["parent_id"] == ObjectId(RESPONSE_MONGOID)
    assert document["parent_ids"] == [ObjectId(RESPONSE_MONGOID)]
    assert document["sk"] == f"{RESPONSE_MONGOID}-{COMMENT_MONGOID}"
    assert document["author_id"] == "11154"
    assert document["votes"] == {
        "up": ["1464"],
        "down": [],
        "up_count": 1,
        "down_count": 0,
        "count": 1,
        "point": 1,
    }
    assert document["endorsement"] == {
        "user_id": "23372",
        "time": datetime(2025, 12, 12, 16, 7, 0, 853844, tzinfo=UTC),
    }
    assert "title" not in document
    assert "author_username" not in document


def test_content_created_after_the_cutover_gets_minted_ids_that_still_join() -> None:
    thread = forum_document(
        {
            **BLANK_ROW,
            "_type": "CommentThread",
            "id": 7,
            "course_id": "course-v1:MITx+7.06r+2015_Fall",
            "author_id": 2,
            "title": "Question 2",
            "body": "How?",
            "thread_type": "question",
            "context": "course",
            "commentable_id": "video_1",
            "closed": False,
            "pinned": False,
            "endorsed": True,
            "comment_count": 1,
            "created_at": CREATED,
            "updated_at": CREATED,
            "last_activity_at": CREATED,
        }
    )
    response = forum_document(_comment())

    assert thread["_id"] == mint_objectid("CommentThread", 7)
    assert response["comment_thread_id"] == thread["_id"]
    assert response["_id"] == mint_objectid("Comment", 42)
    assert response["parent_ids"] == []
    assert response["sk"] == str(response["_id"])
    assert "parent_id" not in response
    assert "endorsement" not in response
    assert "comment_thread_id" not in thread
    # Legacy never stored endorsed on a thread.
    assert "endorsed" not in thread
    assert thread["abuse_flaggers"] == thread["at_position_list"] == []


def test_the_dump_is_back_to_back_bson_in_a_stable_order(tmp_path) -> None:
    rows = [_comment(id=2), _comment(id=1)]
    frame = pl.LazyFrame(
        rows,
        schema_overrides={
            name: pl.List(pl.String)
            for name in (
                "votes_up",
                "votes_down",
                "abuse_flaggers",
                "historical_abuse_flaggers",
            )
        },
    )
    destination = UPath(tmp_path / "contents.bson")

    sha256, size, count = write_forum_bson(frame, destination)

    written = destination.read_bytes()
    documents = bson.decode_all(written)
    assert [document["_id"] for document in documents] == [
        mint_objectid("Comment", 1),
        mint_objectid("Comment", 2),
    ]
    assert sha256 == hashlib.sha256(written).hexdigest()
    assert (size, count) == (len(written), 2)
