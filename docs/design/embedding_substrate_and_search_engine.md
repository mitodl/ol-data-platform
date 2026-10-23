# Embeddings: where they live, where they serve, and how we pick the next one

Status: proposed. Date: 2026-10-09, revised 2026-10-20 when the library in §2 was built (ol-data-platform #3051). Projects: `wp-mit-learn-etl-migration-to-data-platform-bad80f` (Cohort 5, `tk-cohort-5-embedding-pipeline-migration-07fb01`) and the feedback clustering work (`adr_embedding_compute_strategy.md`).

Three questions, answered in order:

1. Should MIT Learn's hybrid search keep running on Qdrant, or move onto StarRocks now that the warehouse engine does vectors and full text?
2. Where should the embeddings themselves be stored?
3. How do we find out which chunking and model choices are the right ones, instead of inheriting them?

Short form: Qdrant stays the serving engine for Learn search, but it stops being the system of record. The platform stores every document, chunk and vector in Iceberg under a named strategy, and Qdrant becomes a projection of one strategy that can be rebuilt or replaced without paying for embeddings again. The same tables are the substrate for comparing strategies. StarRocks as we run it cannot host an ANN index today, so a StarRocks cutover is a later decision with named triggers, not a plan.

Sources were read on 2026-10-09: mit-learn `main` at `6594b8d61`, ol-infrastructure `main` at `5a3263a06`, the StarRocks docs for 4.1 and the 4.0 and 4.1 release notes. Nothing was read from a running Qdrant cluster; see `docs/learn_embedding_config.md` for the full inventory of what Learn writes.

## 1. Qdrant vs. StarRocks for Learn's hybrid search

### What Learn's search needs from the engine

`vector_search/views.py:141-278` builds the query. Per request it uses:

- two named vectors per point: dense `text-embedding-3-large` (cosine) and sparse `bm25` with the IDF modifier;
- a filtered search on each arm, then server-side RRF fusion (`FusionQuery(fusion=RRF)`);
- a `FormulaQuery` rescore on each arm before fusion: proportional boosts gated on payload conditions (program boost), a completeness penalty and a linear datetime-decay staleness penalty (`utils.py:1922-2156`), with per-request weight overrides;
- `query_points_groups` for grouped results, `facet` for aggregation counts (`utils.py:1608-1638`), scroll with `order_by`, `score_threshold`, binary quantization with rescoring, `hnsw_ef`, `indexed_only`;
- strict mode: filters and payload updates on unindexed keys are refused;
- the hit payload _is_ the API response (`VECTOR_SEARCH_RESOURCES_FROM_PAYLOAD=True`), so no database read per hit.

Consumers reach all of this through Learn's API, not Qdrant: learn-ai calls `vector_content_files_search` and `vector_learning_resources_search` (learn-ai `main/settings.py:630-669`). OpenSearch indexing copies the dense vector out of the `resources` collection (`indexing_api.py:382-395`) into a `knn_vector` field, and Learn also defines an OpenSearch hybrid pipeline with min-max normalization (`learning_resources_search/constants.py:28-38`). So today the same vector is held in Qdrant and OpenSearch, and the only copy that is not derived is the one in Qdrant.

### What StarRocks offers, as deployed

We run StarRocks 4.1.6 (`bridge/lib/versions.py:84`). CI, QA and Production all set `starrocks:use_cn: "true"` and `use_be: "false"` (`applications/starrocks/Pulumi.lakehouse.*.yaml`), which the program turns into `run_mode = shared_data` (`__main__.py:813-881`): CN nodes with S3-backed tablet storage, autoscaled 2-20 in Production.

Vector index. The docs for the vector index (HNSW and IVFPQ, `approx_cosine_similarity` / `approx_l2_distance`, one index per table, `ARRAY<FLOAT>` with a declared `dim`, filtered search and range search) open with: "The vector index feature is only supported in shared-nothing clusters of v3.4 or later." It is marked beta and gated behind `enable_experimental_vector`. The 4.0.x and 4.1.x release notes mention the vector index only in bug fixes (4.1.6 compaction SIGFPE on a vector-index rowset, 4.1.5 query cache digest); neither adds shared-data support. Exact `cosine_similarity` over an `ARRAY<FLOAT>` column works on any topology but is a full scan.

Full text. The GIN inverted index exists since 3.3, Primary Key tables since 4.0, and the built-in implementation (4.1) runs on shared-data clusters. It supports `MATCH`, `MATCH_ANY`, `MATCH_ALL` with `english` / `standard` / `chinese` parsers. It is a filter. There is no BM25, TF-IDF or any relevance score, and no fusion primitive.

So on our clusters StarRocks today gives: exact brute-force dense similarity, boolean keyword matching, and everything else Learn does in the formula and grouping layers expressed as ordinary SQL against the dimensional tables rather than a denormalized payload. It does not give an ANN index, a scored lexical arm, or RRF. Moving Learn's hybrid search onto it now would mean either a shared-nothing BE cluster dedicated to serving (a second StarRocks topology to operate) plus a hand-built lexical scorer, or a worse search.

### What Qdrant has cost us

The problems are not with the engine's search quality. They are with where it sits:

- The vectors exist only in Qdrant Cloud. Learn's `resources` payload is DRF serializer output keyed by Learn database ids, the content-file chunks and checksums live only in the payload, and the OpenSearch copy is read back out of Qdrant. A collection recreate, a model change or a second consumer each means re-paying the embeddings API for the whole corpus. The `generate_embeddings` management command with `--recreate` is the only rebuild path.
- The sparse arm is computed by Qdrant Cloud inference (`encoders/qdrant_cloud.py`, `cloud_inference=True`). Local Learn uses a hashing encoder instead (`settings.py:819-824`), so local search is not production search, and the sparse vectors cannot be reproduced anywhere but the vendor.
- The dense model is pinned by code, not config: `hybrid search only supported with OpenAI models` (`learning_resources_search/api.py:769`).
- Operating the collections is Learn application code: optimizer thresholds computed from point counts and re-applied daily (`#3273`, `tune_qdrant_collections`), the IDF modifier switched on by hand on the live collections (`#3947`, and the comment in `utils.py:279-283`), gRPC `DEADLINE_EXCEEDED` and `UNAVAILABLE` retries with minutes-scale backoff (`#3520`, `#3595`), OOM thrashing in the embedding tasks (`#3561`), 200 tasks/minute rate limiting, a weekly healthcheck that reports points that went missing.
- Sizing is opaque. Production is 3 x `mx2` (1 vCPU, 8 GiB) nodes with the disk raised to 204 GiB each (`infrastructure/qdrant_cloud/Pulumi.mitlearn.Production.yaml`); the point counts were not read for this document. The spend is inside the AWS Marketplace line of the FY27 budget and was not broken out.

Of these, only the last two are properties of Qdrant Cloud as a product. The first three are consequences of the embedding pipeline living in the serving store, and they would follow us to StarRocks unchanged if we moved the pipeline the same way.

### Comparison

| | Qdrant (Cloud, as used) | StarRocks 4.1 shared-data (as deployed) |
|---|---|---|
| ANN dense search | HNSW, quantization, named vectors, filtered | not available on shared-data; exact scan only |
| Lexical arm | BM25 sparse with IDF, computed by vendor inference | GIN boolean match, no score |
| Fusion and rescoring | RRF and formula queries server-side | SQL (`UNION`, window functions); straightforward |
| Filters and facets | payload indexes, strict mode | any column, any join, no index to declare |
| Grouping, order_by, pagination | engine features with per-key limits | SQL |
| Where the payload comes from | denormalized per point, written by the app | the dimensional and integrations tables already there |
| Multi-model side by side | named vectors in one collection | one vector index per table; several tables |
| Rebuild cost | full re-embed unless vectors are stored elsewhere | load from Iceberg |
| Maturity for this use | serving Learn since November 2024, hybrid since April 2026 | vector index beta, shared-nothing only; GIN beta |
| Operations | vendor-managed nodes; collection tuning in app code | ours; QA Iceberg read-after-write still open (`tk-make-starrocks-iceberg-reads-see-their-own-write-802810`) |
| Vendor coupling | hosted, sparse inference, client pins | none |

### Decision

Keep Qdrant as the serving engine for Learn search. Do not plan a StarRocks search cutover. Instead change what Qdrant is: a projection of platform-owned data, rebuildable from Iceberg, carrying one named strategy at a time (or two during a cutover, as a second named vector).

Two Qdrant-side changes are worth making regardless, and neither needs the platform:

- Compute sparse vectors client-side with `fastembed`'s `Qdrant/bm25` instead of cloud inference. Learn already pins `qdrant-client[fastembed]`. The IDF modifier stays a collection setting, so search behaviour is unchanged, and the sparse vectors become reproducible by anything that can import fastembed, including the platform. (Judgment call: I have not confirmed that the cloud `qdrant/bm25` and fastembed's `Qdrant/bm25` tokenize identically; the cutover should diff a sample.)
- Self-hosted Qdrant on EKS is the same engine under an Apache-2.0 licence. It removes the Marketplace line and the vendor-side inference, at the cost of running three stateful pods. Not proposed here; it becomes attractive only once the vectors are rebuildable from Iceberg, because today a self-hosting migration is itself a full re-embed.

Re-evaluate StarRocks for serving when all of these hold: the vector index is supported on shared-data clusters or we decide to run a shared-nothing serving cluster; a scored lexical arm exists in StarRocks or we are willing to serve dense-only with lexical as a filter; and the strategy registry below has a production arm whose vectors already sit in Iceberg, so the trial is a load and an eval, not a project. Until then, StarRocks' role for embeddings is the one the feedback ADR already gives it: exact-similarity SQL over Iceberg arrays for offline evaluation and clustering.

## 2. Embeddings as platform data

### Principle

The platform owns the text that gets embedded, the chunking, and the vectors. Every one of those is a table in Iceberg with a business key and a strategy id. Serving stores (Qdrant now, anything later) are fed from those tables and can be dropped and refilled. This is the same shape `feedback_embeddings` already has (`dg_projects/ml/ml/lib/embed.py:24-36`: vector, dim, model version, `embedded_at`, upsert key including model and dim) generalized across document kinds.

### Tables

All in the `integrations` layer's schema, Dagster-written (the embedding call is engine-external, per `adr_embedding_compute_strategy.md`), with dbt models over them where SQL is enough.

`embedding_documents`: one row per thing we might embed, per document builder version.

| column | notes |
|---|---|
| `doc_key` | Learn's point key string, so ids can be compared with Qdrant point by point: `{platform}.{readable_id}`, `{platform}.{resource_readable_id}.{run_readable_id}.{key}`, `{platform}.{readable_id}.course_metadata`, topic uuid (`docs/learn_embedding_config.md`, "Point ids") |
| `doc_kind` | `learning_resource`, `content_file`, `resource_metadata`, `topic`, `feedback_conversation` |
| `builder_id` | which rendering produced `text` (e.g. `learn_resource_markdown_v1`, `content_file_raw_v1`) |
| `text` | the embedding input, after visibility rules; for content files this is `integrations__learn__content_files.content`, never an earlier layer (#2812) |
| `text_checksum` | md5 of `text`; Learn's content-file `checksum` where the builder is `content_file_raw_v1`, so parity is checkable |
| `source_relation`, `source_key` | the dbt model and row this came from |
| `etl_source`, `platform_code` | partition columns |
| `retrieved_on` | |

This is a dbt model per `doc_kind` (`int__embedding__learn_content_files` etc.) unioned into one, because it is a pure projection of the integrations models. The resource metadata markdown is the exception: it is Learn serializer output, and until Learn exposes it (or the platform renders it from the catalog models) the resources arm reads text Learn supplies. That is open decision 2 of `learn_embedding_config.md` and does not block content files or topics.

`embedding_chunks`: one row per chunk per chunker.

| column | notes |
|---|---|
| `chunk_key` | `uuid5(doc_key || chunker_id || chunk_number)` |
| `doc_key`, `doc_kind` | |
| `chunker_id` | derived from the chunker's parameters, e.g. `token:cl100k_base:512:51:md4` (Learn's content files), `token:cl100k_base:1024:100`, `whole:cl100k_base:8191` |
| `chunk_number` | gaps allowed, as Learn's are |
| `chunk_text`, `chunk_checksum` | |

Chunking is cheap and deterministic, so it is recomputed when `text_checksum` changes and never when it does not. A chunker is a pydantic model of its parameters, and `chunker_id` is built from them, so changing a parameter is a new id rather than a silent change to existing rows.

`embedding_vectors`: one row per chunk per strategy.

| column | notes |
|---|---|
| `chunk_key` | |
| `strategy_id` | |
| `model_id` | ol-llm model id and vector length, e.g. `openai:text-embedding-3-large@3072`, `openai:text-embedding-3-large@1024`, `google:gemini-embedding-001@768`, `bedrock:amazon.titan-embed-text-v2:0@1024` |
| `vector_space` | which vectors are interchangeable: the strategy model's declared `vector_space` (e.g. `text-embedding-3-large@3072` for OpenAI's model and an Azure OpenAI deployment of it), else its `model_id` |
| `vector` | `ARRAY<FLOAT>` (float32) |
| `input_checksum` | MD5 of the exact text sent to the model: the chunk after the strategy's context template. A mismatch with the current text means re-embed |
| `embedded_at`, `run_id` | `run_id` is the Dagster run, added by the asset |

Upsert key `(chunk_key, strategy_id)`. The key is the strategy and not the model because a context template changes the vector without changing the model or the chunk. A new strategy adds rows; it never overwrites (the mistake recorded in `tk-feedback-embeddings-the-per-run-model-dim-overri-1b99d9`).

Embedding cost is bounded by the input checksum instead of a separate cache table. Before calling the model, the asset looks up the vectors already stored for `(input_checksum, vector_space)` and sends only the texts with none. The same OLX file appears in every run of a course and Learn embeds it once per run; the platform pays once per distinct text, and two strategies that send the same text into the same space share the vector. A model is a space of its own unless its spec declares one, so vectors of two providers are mixed only where a strategy says they are the same.

`embedding_sparse_vectors`: same shape with `model_id = fastembed/Qdrant-bm25` and `indices ARRAY<INT>`, `values ARRAY<FLOAT>`. Only needed once Learn computes sparse client-side; until then the Qdrant projection sends `Document(text=...)` and lets the vendor compute it.

`embedding_strategies`: the registry, read from a YAML file in the repo through a pydantic model (`EmbeddingStrategy`), so a strategy is reviewable in a PR. The table is a copy of that file for SQL joins.

| field | notes |
|---|---|
| `strategy_id` | e.g. `learn_v1_content_files`, `exp_2026_10_matryoshka_1024`, `exp_2026_10_gemini` |
| `doc_kind` | |
| `chunker` | the chunker's parameters; `chunker_id` is derived from them, e.g. `token:cl100k_base:512:51:md4` |
| `model` | ol-llm model id, requested vector length, batch size, optional `vector_space`; `model_id` is derived |
| `context_template` | optional `str.format` template, e.g. `"{title}\n\n{chunk}"`, rendered with the document's metadata, for contextual-chunk experiments. The stored and searched chunk text stays the chunk |
| `role` | `serving`, `candidate`, `retired` |
| `qdrant` | collection (the name after Learn's `<base>.`), dense vector name, sparse model and vector name; null for platform-only strategies |
| `notes` | |

`learn_v1` is Learn's current configuration verbatim, one strategy per document kind (`learn_v1_content_files` is built): `text-embedding-3-large` at 3072 (to be confirmed with one `get_collection`), `RecursiveCharacterTextSplitter.from_tiktoken_encoder(cl100k_base, 512, 51)` for content files, header-aware splitting for markdown and marketing pages, `RecursiveJsonSplitter(2048)` for metadata, whole-document for resources and topics. Its purpose is parity: its chunk checksums and point ids must match Qdrant before anything else is trusted.

### The library

`ol_orchestrate.lib.embeddings` (the `embeddings` extra of ol-orchestrate-lib) holds everything above that is not storage:

- `documents`: `Document` (`doc_key`, `doc_kind`, `text`, `format`, `metadata`).
- `chunking`: `TokenChunker` (tiktoken-measured recursive splitting, with optional markdown header splitting) and `WholeDocumentChunker` (optional token cap, which is how Learn truncates resource text). Ids are derived from the parameters.
- `models`: `EmbeddingModelSpec`, the `Embedder` protocol, and `OlLlmEmbedder`, which calls ol-llm's `embed_sync` in batches of `batch_size`. ol-llm supplies the retry policy, error classes, cost accounting and one Opik trace per request; it leaves batching to provider limits to the caller.
- `strategy`: `EmbeddingStrategy`, `QdrantTarget`, `load_strategies(path)`.
- `pipeline`: `chunk_documents` and `embed_chunks`, producing the `ChunkRow` and `VectorRow` shapes of the tables above, with the input-checksum reuse.
- `qdrant`: `build_points`, with a `PointMapper` for the consumer's point ids and payloads.
- `evaluation`: the in-process evaluator of §3 and `log_opik_experiment`.
- `learn`: `LEARN_V1_CONTENT_FILES`, Learn's point keys, `ContentFilePointMapper` and `chunk_and_embed`. Its tests run a copy of Learn's own code against it.

`ol_orchestrate.resources.embeddings` has `EmbeddingsResource` (configures ol-llm and its Opik tracing, with the Keycloak client from Vault `secret-operations/sso/opik`, and gives each strategy an ol-llm use case `embed-<strategy>`) and `QdrantResource`.

### Dagster assets

- `embedding_documents` and `embedding_chunks`: dbt for documents; a Python asset per `doc_kind` for chunks, partitioned by `etl_source` (content files additionally by course run, as the Cohort 5 task already says), with `code_version` tied to the chunker parameters.
- `embedding_vectors`: partitioned by `(strategy_id, etl_source)` with `strategy_id` a dynamic partition so a new strategy is a YAML row, not a code change. Reads chunks, passes the stored vectors for the strategy's model as `known_vectors`, embeds the rest through `EmbeddingsResource`, writes with the Iceberg upsert.
- `qdrant_collection_projection`: partitioned by strategy, for strategies with `role in (serving, candidate)` and a collection set. Reads vectors plus the payload view, builds `PointStruct`s with Learn's UUID5 ids and `(dense vector name, sparse vector name)` from the registry, upserts in Learn's batch sizes, and deletes points whose `doc_key` left the documents table, _except_ for `QDRANT_RETAINED_SOURCES` where Learn keeps points after unpublish (`learn_embedding_config.md`, "When Learn embeds"). Asset check: point count per collection within 1% of the vector row count for that strategy.
- Payload: for `content_files` and `topics` the payload fields are all in the integrations models or derivable (the missing ones, `summary`, `flashcards`, `run_title`, `course_number`, `content_feature_type`, `platform`/`offered_by` objects, are listed in `learn_embedding_config.md`); a `payload_builder` per `doc_kind` is part of the strategy's builder. For `resources` the payload stays Learn's until open decision 2 is resolved.

### Costs and sizes

A 3072-float32 vector is 12 KiB; at 1024 it is 4 KiB. One million chunks at 3072 is 12 GB in Iceberg before compression, which is small next to the raw layer. Embedding spend is the real cost and the text cache plus incremental keys are what bound it: a backfill of an arm is paid once, and a re-run after a chunk text change embeds only the changed chunks.

## 3. The experimentation substrate

Everything above makes an arm cheap to add. What makes it cheap to _judge_ is three more things.

### An evaluation set that is ours

`search_eval_queries`: `(eval_set_id, query_id, query_text, doc_kind, relevant_doc_keys ARRAY<STRING>, grade, origin)`.

Three origins, in order of cost:

1. Synthetic: for a sample of documents, an LLM writes the question that document answers; the generating document is the relevant one. Cheap, thousands of queries, good for recall and for catching a chunker that splits an answer in two. Biased toward the model's phrasing; use for ranking arms, not for absolute numbers.
2. Behavioural: Learn's search requests and the resource the user opened afterwards, where that log exists (not checked for this document). Noisy but real.
3. Hand-labelled: a few hundred queries graded by the Learn team. The only set that can veto a cutover.

Redaction applies to any feedback-conversation set exactly as the feedback ADR requires.

### An evaluator that measures retrieval, not Qdrant

`embedding_eval_results`: `(strategy_id, eval_set_id, metric, value, k, evaluated_at, run_id)`.

Three executors, used at different stages:

- In-process exact search (`evaluate_retrieval`): load the arm's vectors for the eval set's `doc_kind` into memory (1M x 1024 float32 is 4 GB, and the search holds one normalized copy, so about 8 GB in all), embed the queries with the arm's model as `query` inputs, rank documents by their best chunk's cosine similarity, and score recall@k, nDCG@k and MRR at the deepest k. No index approximation, no quantization, no RRF: this isolates the chunker, the context template and the model, which is what a bake-off is for. The feedback clustering work already does this shape of computation. `log_opik_experiment` records the result as an Opik experiment on a dataset holding one version of the eval set (one item per query; the dataset name carries a hash of the items, because Opik scores every item in a dataset), with the strategy as the experiment configuration, so Opik's experiment comparison lines strategies up query by query. A report carries the same hash, and `log_opik_experiment` refuses one computed on another version of the set.
- StarRocks exact SQL: `cosine_similarity(vector, [...])` over the Iceberg table. Same answer as above, and it is the way a dashboard or an analyst can run an ad-hoc comparison without a Dagster run. This is where StarRocks earns a place in the embedding story today.
- Qdrant candidate collection: the projection asset fills `<base>.<collection>__<strategy_id>` and the evaluator runs Learn's actual query shape against it (RRF, quantization, boosts). This is the pre-cutover check and the only executor that measures latency honestly.

Cost per arm (tokens x price, logged by the embedder) goes in the same results table so the dashboard shows quality next to spend.

### A promotion path that is one row

An arm becomes production by setting `role = serving` and pointing it at the live collection with a new `qdrant_vector_name`. Because Qdrant collections carry several named vectors, the projection writes the new vector alongside the old on the same points; Learn flips `QDRANT_DENSE_MODEL`; the old vector name is dropped after. No collection recreate, no downtime, and rollback is flipping the setting back. This is a Qdrant strength that StarRocks (one vector index per table) does not have, and another reason not to move the serving tier while we are still choosing models.

### First arms worth running

- `learn_v1` as the baseline (and the parity gate).
- `text-embedding-3-large@1024` via the `dimensions` parameter: same model, a third of the storage, and the Matryoshka result the feedback work already assumes. If it ties the baseline on the hand-labelled set, it is the cheap win.
- `gemini-embedding-001` or Cohere `embed-v4`, the managed candidates the feedback bake-off shortlisted, so the two bake-offs share a model list.
- One chunker variant: 1024/100 for content files, and whole-section chunks for OLX problems, which the question/answer separation RFC will need anyway.

## Phasing

0. Parity (no Learn change). Documents, chunks and `learn_v1` vectors for content files and topics. Diff chunk checksums and point ids against Qdrant for mitxonline, xpro and OCW. Answer the 3072 question. Closes Cohort 5's `tk-document-current-embedding-configuration-learn-e-385462` on the way.
1. Substrate. Registry, eval sets (synthetic first), in-process evaluator, results table, Superset dashboard. Run the four arms above.
2. Reflect. Projection asset writes `<base>.platform_content_files` and `platform_topics`; Learn reads them behind a setting; cut over; Learn stops embedding content files and topics. Learn moves sparse to client-side fastembed in the same window so the platform can carry the sparse arm too.
3. Resources. Resolve who renders the resource markdown and who writes the payload (open decision 2), then the same path. Fold `feedback_embeddings` onto the shared tables.
4. Serving engine review. Triggered by the conditions in §1, with the arm already in Iceberg so the trial is a load.

## Open questions for the Learn team

1. Separate collections for the first platform-written arms (recommended in `learn_embedding_config.md`), then rename at cutover: agreed?
2. Client-side `fastembed` BM25 in Learn's encoder, replacing cloud inference: acceptable, and will the Learn team own that change?
3. Does a search-click log exist that the behavioural eval set could be built from?
4. Who grades the hand-labelled set, and how many queries is enough to veto a cutover?
5. The resources payload: Learn keeps writing it, the platform writes only vectors, or Learn stops serving hits from the payload. The platform can proceed through phase 2 without an answer.
