"""Documents, chunks and vectors for retrieval, under named, comparable strategies.

A strategy (:mod:`.strategy`) names the three choices that decide a vector: the
chunker (:mod:`.chunking`), the embedding model (:mod:`.models`), and the context
template that decorates each chunk before it is embedded. :mod:`.pipeline` turns
documents into chunk rows and vector rows keyed by those choices, so vectors from
two strategies sit side by side and a change of model adds rows instead of
replacing them. :mod:`.qdrant` projects vector rows into a Qdrant collection,
:mod:`.evaluation` scores a strategy's retrieval and records it as an Opik
experiment, and :mod:`.learn` is MIT Learn's current configuration expressed in
these terms. ``docs/design/embedding_substrate_and_search_engine.md`` is the
design. Requires the ``embeddings`` extra.
"""
