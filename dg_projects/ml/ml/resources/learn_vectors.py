"""Learn's vectors of the texts it embedded, read from its Qdrant collections.

The vector assets take Learn's vector for a chunk whose text Learn has already
embedded, rather than paying to embed it again. Learn's point ids are the
platform's, so a chunk's point is read by id, and its vector is taken only when
the text Learn embedded is the chunk's (see
:func:`ml.lib.learn_embeddings.learn_vectors`). Dev has no Learn cluster, so the
resource there has no client and the vector assets embed every new text.
"""

from collections.abc import Callable, Mapping, Sequence
from functools import partial

from dagster import ConfigurableResource
from ml.lib.learn_parity import LearnPoint, retrieve_points
from ol_orchestrate.lib.embeddings.strategy import QdrantTarget
from ol_orchestrate.resources.embeddings import QdrantResource
from pydantic import Field

PointFetch = Callable[[Sequence[str]], Mapping[str, LearnPoint]]


class LearnVectorsResource(ConfigurableResource[None]):
    """Read Learn's points by id, or nothing where there is no Learn cluster."""

    qdrant: QdrantResource | None = Field(
        default=None,
        description="Learn's cluster, read-only; unset in dev, which has none",
    )

    def fetch(self, target: QdrantTarget, text_field: str) -> PointFetch | None:
        """Return a reader of a Learn collection's points by id, ``None`` in dev.

        :param target: Learn's collection, e.g. ``LEARN_CONTENT_FILES_COLLECTION``
        :param text_field: the payload key of the text Learn embedded
        :rtype: PointFetch | None
        """
        if self.qdrant is None:
            return None
        return partial(
            retrieve_points,
            self.qdrant.get_client(),
            self.qdrant.collection_name(target),
            text_field=text_field,
            checksum_field=None,
            vector_name=target.dense_vector_name,
        )
