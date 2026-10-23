"""Compare the platform's learn_v1 chunks and vectors with Learn's Qdrant points.

The platform's point ids are Learn's, so each chunk is looked up in Learn's
collection by id. Learn's payload carries the exact text it embedded
(``chunk_content`` for a content file, ``name`` for a topic) and, for a content
file, the file's checksum, so text, checksum and vector are compared point by
point. A chunk whose text matches embeds the same input on both sides, and both
sides call text-embedding-3-large, so its two vectors should be the same up to
the providers' numerical noise.
"""

from collections import defaultdict
from collections.abc import Callable, Collection, Mapping, Sequence
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
class CosineStats:
    """Cosine similarities of the compared vectors of one model.

    :param compared: vectors compared
    :param minimum: the least similarity, ``None`` with none compared
    :param p50: the median similarity, ``None`` with none compared
    :param below_threshold: similarities under ``MIN_COSINE_SIMILARITY``
    """

    compared: int
    minimum: float | None
    p50: float | None
    below_threshold: int

    @classmethod
    def of(cls, similarities: Sequence[float]) -> "CosineStats":
        """Summarize similarities.

        :param similarities: the similarities
        :rtype: CosineStats
        """
        return cls(
            compared=len(similarities),
            minimum=min(similarities) if similarities else None,
            p50=float(np.median(similarities)) if similarities else None,
            below_threshold=sum(
                similarity < MIN_COSINE_SIMILARITY for similarity in similarities
            ),
        )


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
    :param cosine_by_model: the similarities of text-matching chunks' vectors,
        by the ``model_id`` of the platform's vector row
    :param reference_model_id: Learn's own model. A platform row of it holds a
        vector imported from Learn, so its similarity is reported but does not
        make the vectors differ.
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
    cosine_by_model: Mapping[str, CosineStats]
    reference_model_id: str

    @property
    def compared_vectors(self) -> int:
        """Vectors compared, over every model.

        :rtype: int
        """
        return sum(stats.compared for stats in self.cosine_by_model.values())

    @property
    def below_threshold(self) -> int:
        """Vectors under the threshold from models other than Learn's own.

        :rtype: int
        """
        return sum(
            stats.below_threshold
            for model_id, stats in self.cosine_by_model.items()
            if model_id != self.reference_model_id
        )

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


def _dense(vector: object) -> list[float] | None:
    # A named vector comes back as one dense vector, a list of them (multivector)
    # or a sparse one; Learn's dense vector is a single one.
    if vector is None:
        return None
    if not isinstance(vector, list) or (vector and not isinstance(vector[0], float)):
        msg = f"expected one dense vector, got {type(vector).__name__}"
        raise TypeError(msg)
    return vector


def retrieve_points(  # noqa: PLR0913
    client: QdrantClient,
    collection: str,
    ids: Sequence[str],
    *,
    text_field: str,
    checksum_field: str | None,
    vector_name: str,
) -> dict[str, LearnPoint]:
    """Read Learn's points by id, in one request.

    :param client: a client for Learn's cluster
    :param collection: the collection
    :param ids: the point ids, at most ``QDRANT_BATCH_SIZE``
    :param text_field: the payload key of the embedded text
    :param checksum_field: the payload key of the checksum, ``None`` for none
    :param vector_name: the dense vector's name
    :returns: the points found, by id
    :rtype: dict[str, LearnPoint]
    """
    fields = [text_field] if checksum_field is None else [text_field, checksum_field]
    points = {}
    for record in client.retrieve(
        collection, ids=ids, with_payload=fields, with_vectors=[vector_name]
    ):
        payload = record.payload or {}
        vectors = record.vector if isinstance(record.vector, dict) else {}
        points[str(record.id)] = LearnPoint(
            text=payload.get(text_field),
            checksum=payload.get(checksum_field) if checksum_field else None,
            # Learn writes a point with a zero dense vector without vectors.
            vector=_dense(vectors.get(vector_name)),
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


def compare_with_learn(  # noqa: PLR0913
    platform: pl.DataFrame,
    fetch: Callable[[Sequence[str]], Mapping[str, LearnPoint]],
    learn_ids: Collection[str],
    *,
    compare_checksums: bool,
    reference_model_id: str,
    batch_size: int = QDRANT_BATCH_SIZE,
) -> ParityReport:
    """Compare the platform's chunks with Learn's points, a batch of ids at a time.

    Only the counts and similarities of a batch are kept, so a run's vectors are
    never all held as Python lists.

    :param platform: ``point_id``, ``chunk_text``, ``text_checksum``, ``vector``
        and ``model_id`` (both null without a vector row) per chunk
    :param fetch: Learn's points for some ids, e.g. :func:`retrieve_points`
    :param learn_ids: Learn's point ids in the same scope, from
        :func:`scroll_point_ids`
    :param compare_checksums: whether Learn's points carry the file checksum
    :param reference_model_id: Learn's own model, see ``ParityReport``
    :param batch_size: ids per ``fetch``
    :rtype: ParityReport
    """
    found: set[str] = set()
    matched = text_matches = checksum_matches = without_vector = 0
    similarities: dict[str, list[float]] = defaultdict(list)
    for offset in range(0, platform.height, batch_size):
        rows = platform.slice(offset, batch_size)
        learn = fetch(rows["point_id"].to_list())
        found.update(learn)
        for row in rows.iter_rows(named=True):
            point = learn.get(row["point_id"])
            if point is None:
                continue
            matched += 1
            checksum_matches += point.checksum == row["text_checksum"]
            without_vector += point.vector is None
            if point.text != row["chunk_text"]:
                continue
            text_matches += 1
            if row["vector"] is None or point.vector is None:
                continue
            if (cosine := _cosine(row["vector"], point.vector)) is not None:
                similarities[row["model_id"]].append(cosine)
    platform_ids = set(platform["point_id"])
    return ParityReport(
        platform_chunks=platform.height,
        platform_chunks_without_vector=platform["vector"].null_count(),
        learn_points=len(set(learn_ids) | found),
        matched_ids=matched,
        missing_in_learn=len(platform_ids - found),
        missing_on_platform=len(set(learn_ids) - platform_ids),
        text_matches=text_matches,
        checksum_matches=checksum_matches if compare_checksums else None,
        learn_points_without_vector=without_vector,
        cosine_by_model={
            model_id: CosineStats.of(values)
            for model_id, values in sorted(similarities.items())
        },
        reference_model_id=reference_model_id,
    )
