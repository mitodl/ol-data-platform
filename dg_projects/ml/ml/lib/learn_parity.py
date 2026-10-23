"""Compare the platform's learn_v1 chunks and vectors with Learn's Qdrant points.

The platform's point ids are Learn's, so each chunk is looked up in Learn's
collection by id. Learn's payload carries the exact text it embedded
(``chunk_content`` for a content file, ``name`` for a topic) and, for a content
file, the file's checksum, so text, checksum and vector are compared point by
point. A chunk whose text matches embeds the same input on both sides, and both
sides call text-embedding-3-large, so its two vectors should be the same up to
the providers' numerical noise.
"""

from collections.abc import Collection, Iterator, Mapping, Sequence
from dataclasses import dataclass

import numpy as np
import polars as pl
from qdrant_client import QdrantClient, models

# Vectors of the same text from the same model differ only in noise, far above
# this. A lower similarity means the two sides did not embed the same way.
MIN_COSINE_SIMILARITY = 0.99
QDRANT_BATCH_SIZE = 256
# Learn's per-resource metadata chunks share the content files collection; the
# platform does not build them (docs/learn_embedding_config.md).
COURSE_METADATA_FILE_TYPE = "course_metadata"


@dataclass(frozen=True)
class LearnPoint:
    """What Learn stores on a point, as far as the comparison reads it.

    :param text: the text Learn embedded
    :param checksum: the payload ``checksum``, ``None`` for a collection without
        one
    :param vector: the dense vector, ``None`` when the point has none
    """

    text: str | None
    checksum: str | None
    vector: list[float] | None


@dataclass(frozen=True)
class ParityReport:
    """How a set of platform chunks compares with Learn's points.

    :param platform_chunks: the platform's chunks
    :param platform_chunks_without_vector: of those, chunks with no vector row
    :param learn_points: Learn's points in scope, found by id or by filter
    :param matched_ids: platform chunks whose point id Learn has
    :param missing_in_learn: platform chunks whose point id Learn does not have
    :param missing_on_platform: Learn's points in scope with no platform chunk
    :param text_matches: matched chunks whose text is Learn's
    :param checksum_matches: matched chunks whose ``text_checksum`` is Learn's
        payload checksum, ``None`` for a collection without checksums
    :param learn_points_without_vector: matched points with no dense vector
    :param compared_vectors: text-matching chunks whose vectors were compared
    :param cosine_min: least cosine similarity of the compared vectors
    :param cosine_p50: median cosine similarity of the compared vectors
    :param below_threshold: compared vectors under ``MIN_COSINE_SIMILARITY``
    """

    platform_chunks: int
    platform_chunks_without_vector: int
    learn_points: int
    matched_ids: int
    missing_in_learn: int
    missing_on_platform: int
    text_matches: int
    checksum_matches: int | None
    learn_points_without_vector: int
    compared_vectors: int
    cosine_min: float | None
    cosine_p50: float | None
    below_threshold: int

    @property
    def vectors_differ(self) -> bool:
        """Whether a chunk with Learn's text has a vector unlike Learn's.

        :rtype: bool
        """
        return self.below_threshold > 0

    @property
    def points_differ(self) -> bool:
        """Whether the two sides have different points or texts, or a chunk lacks
        a vector.

        Different points and texts are expected for mitxonline and xpro OLX
        files, whose text the platform approximates. A chunk without a vector is
        one whose embedding request failed.

        :rtype: bool
        """
        return bool(
            self.platform_chunks_without_vector
            or self.missing_in_learn
            or self.missing_on_platform
            or self.text_matches < self.matched_ids
            or (self.platform_chunks and not self.compared_vectors)
        )


def _batches(ids: Sequence[str], size: int) -> Iterator[Sequence[str]]:
    for start in range(0, len(ids), size):
        yield ids[start : start + size]


def retrieve_points(  # noqa: PLR0913
    client: QdrantClient,
    collection: str,
    ids: Sequence[str],
    *,
    text_field: str,
    checksum_field: str | None,
    vector_name: str,
    batch_size: int = QDRANT_BATCH_SIZE,
) -> dict[str, LearnPoint]:
    """Read Learn's points by id, ``batch_size`` ids per request.

    :param client: a client for Learn's cluster
    :param collection: the collection
    :param ids: the point ids
    :param text_field: the payload key of the embedded text
    :param checksum_field: the payload key of the checksum, ``None`` for none
    :param vector_name: the dense vector's name
    :param batch_size: ids per request
    :returns: the points found, by id
    :rtype: dict[str, LearnPoint]
    """
    fields = [text_field] if checksum_field is None else [text_field, checksum_field]
    points = {}
    for batch in _batches(ids, batch_size):
        for record in client.retrieve(
            collection, ids=batch, with_payload=fields, with_vectors=[vector_name]
        ):
            payload = record.payload or {}
            vectors = record.vector if isinstance(record.vector, dict) else {}
            # Learn writes a point with a zero dense vector without vectors.
            vector = vectors.get(vector_name)
            points[str(record.id)] = LearnPoint(
                text=payload.get(text_field),
                checksum=payload.get(checksum_field) if checksum_field else None,
                vector=list(vector) if isinstance(vector, list) else None,
            )
    return points


def scroll_point_ids(
    client: QdrantClient,
    collection: str,
    scroll_filter: models.Filter | None,
    *,
    batch_size: int = QDRANT_BATCH_SIZE,
) -> set[str]:
    """Return the ids of the points a filter selects, ``batch_size`` per page.

    Metadata chunks are left out by their ``file_type``, read from the payload:
    Learn's collections refuse a filter on a key without a payload index, and
    ``file_type`` has none (docs/learn_embedding_config.md).

    :param client: a client for Learn's cluster
    :param collection: the collection
    :param scroll_filter: the points wanted, ``None`` for all of them; it may
        only use Learn's indexed payload keys
    :param batch_size: points per page
    :rtype: set[str]
    """
    ids: set[str] = set()
    offset = None
    while True:
        records, offset = client.scroll(
            collection,
            scroll_filter=scroll_filter,
            limit=batch_size,
            offset=offset,
            with_payload=["file_type"],
            with_vectors=False,
        )
        ids.update(
            str(record.id)
            for record in records
            if (record.payload or {}).get("file_type") != COURSE_METADATA_FILE_TYPE
        )
        if offset is None:
            return ids


def content_file_run_filter(platform_code: str, run_readable_id: str) -> models.Filter:
    """Select a course run's points, on keys Learn indexes.

    :param platform_code: the run's platform, the payload's ``platform.code``
    :param run_readable_id: the run, as the payload's ``run_readable_id``
    :rtype: models.Filter
    """
    return models.Filter(
        must=[
            models.FieldCondition(
                key="platform.code", match=models.MatchValue(value=platform_code)
            ),
            models.FieldCondition(
                key="run_readable_id", match=models.MatchValue(value=run_readable_id)
            ),
        ],
    )


def _cosine(left: Sequence[float], right: Sequence[float]) -> float | None:
    a = np.asarray(left, dtype=np.float64)
    b = np.asarray(right, dtype=np.float64)
    norm = float(np.linalg.norm(a) * np.linalg.norm(b))
    return float(a @ b) / norm if norm else None


def compare_with_learn(
    platform: pl.DataFrame,
    learn: Mapping[str, LearnPoint],
    learn_ids: Collection[str],
    *,
    compare_checksums: bool,
) -> ParityReport:
    """Compare the platform's chunks with Learn's points, id by id.

    :param platform: ``point_id``, ``chunk_text``, ``text_checksum`` and
        ``vector`` (null without a vector row) per chunk
    :param learn: Learn's points for the platform's ids, from
        :func:`retrieve_points`
    :param learn_ids: Learn's point ids in the same scope, from
        :func:`scroll_point_ids`
    :param compare_checksums: whether Learn's points carry the file checksum
    :rtype: ParityReport
    """
    platform_ids = set(platform["point_id"])
    matched = [
        row for row in platform.iter_rows(named=True) if row["point_id"] in learn
    ]
    text_matched = [
        row for row in matched if learn[row["point_id"]].text == row["chunk_text"]
    ]
    cosines = [
        _cosine(row["vector"], vector)
        for row in text_matched
        if row["vector"] is not None
        and (vector := learn[row["point_id"]].vector) is not None
    ]
    similarities = [cosine for cosine in cosines if cosine is not None]
    return ParityReport(
        platform_chunks=platform.height,
        platform_chunks_without_vector=platform["vector"].null_count(),
        learn_points=len(set(learn_ids) | set(learn)),
        matched_ids=len(matched),
        missing_in_learn=len(platform_ids - set(learn)),
        missing_on_platform=len(set(learn_ids) - platform_ids),
        text_matches=len(text_matched),
        checksum_matches=sum(
            learn[row["point_id"]].checksum == row["text_checksum"] for row in matched
        )
        if compare_checksums
        else None,
        learn_points_without_vector=sum(
            learn[row["point_id"]].vector is None for row in matched
        ),
        compared_vectors=len(similarities),
        cosine_min=min(similarities) if similarities else None,
        cosine_p50=float(np.median(similarities)) if similarities else None,
        below_threshold=sum(
            similarity < MIN_COSINE_SIMILARITY for similarity in similarities
        ),
    )
