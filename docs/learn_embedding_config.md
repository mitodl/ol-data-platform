# MIT Learn embedding configuration

MIT Learn writes three Qdrant collections from Celery tasks in `vector_search/`. Cohort 5 of the Learn ETL migration moves that work to the data platform. This document records what Learn does today, so that anything the platform writes can be compared with it, and lists what has to be decided before the platform assets are built.

Read from mit-learn `main` at `236023503` and ol-infrastructure `main` at `df1f06a4d` on 2026-10-06. File references are to mit-learn unless they say otherwise. Nothing here was read from a running Qdrant cluster. The values under "Deployed settings" are what the Pulumi program sets, not what a pod reports.

## Deployed settings

Set for QA and Production in ol-infrastructure `src/ol_infrastructure/applications/mit_learn/__main__.py` (lines 1529-1585). Stack config is applied over that dict (`__main__.py:1632`), and the CI stack overrides `QDRANT_DENSE_MODEL` to `text-embedding-3-small` (`Pulumi.CI.yaml:89`), so CI vectors have a different model and vector name. The defaults in `main/settings.py` are different (a local gensim encoder and a hashing sparse encoder), so a local Learn stack does not reproduce production vectors.

| Setting | Deployed value | Effect |
|---|---|---|
| `QDRANT_ENCODER` | `vector_search.encoders.litellm.LiteLLMEncoder` | dense vectors come from an embeddings API call through LiteLLM |
| `QDRANT_DENSE_MODEL` | `text-embedding-3-large` | dense model, and the name of the dense vector |
| `QDRANT_SPARSE_ENCODER_V2` | `vector_search.encoders.qdrant_cloud.QdrantCloudEncoder` | sparse vectors are computed by Qdrant Cloud inference, not by Learn |
| `QDRANT_SPARSE_MODEL_V2` | `qdrant/bm25` | sparse model; the sparse vector is named `bm25` |
| `QDRANT_COLLECTION_NAME` | `mitlearn-<stack>` (e.g. `mitlearn-production`) | base name of the three collections |
| `QDRANT_HOST` | `cluster_url` output of the `mitlearn.<stack>` Qdrant Cloud stack | |
| `QDRANT_API_KEY` | Vault `qdrant` secret, key `api_key_v2` (`k8s_secrets.py:198`) | |
| `LITELLM_TOKEN_ENCODING_NAME` | `cl100k_base` | chunk sizes are counted in tokens of this encoding |
| `CONTENT_FILE_EMBEDDING_CHUNK_SIZE` | `512` | |
| `CONTENT_FILE_EMBEDDING_CHUNK_OVERLAP` | `51` | |
| `EMBEDDING_SCHEDULE_MINUTES` | `120` | period of the two "embed new" beat tasks |
| `QDRANT_ENABLE_INDEXING_PLUGIN_HOOKS` | `True` | resource and content-file writes queue embedding tasks |
| `QDRANT_CHUNK_SIZE` | `8` (QA and Production stack config) | ids per `generate_embeddings` task |

`LITELLM_CUSTOM_PROVIDER` and `LITELLM_API_BASE` are not set by the Pulumi program, so the settings defaults apply: provider `openai`, no API base. The key is `OPENAI_API_KEY` from the Vault `openai` secret.

Library pins in mit-learn `pyproject.toml`: `litellm==1.96.2`, `qdrant-client[fastembed]~=1.18.0`, `tiktoken>=0.13,<0.14`, `langchain-text-splitters>=1.1.2`.

## Collections

`vector_search/constants.py:4-6` names them from the base name:

- `<base>.resources`: one point per learning resource.
- `<base>.content_files`: one point per chunk of a content file, plus the chunks of each resource's metadata document.
- `<base>.topics`: one point per topic name.

All three are created with the same parameters (`vector_search/utils.py:259-303`):

- a named dense vector, cosine distance, sized by embedding the string `test` and taking the length of the result (`encoders/base.py:34-38`). OpenAI documents 3072 as the default length for `text-embedding-3-large`; the live collection's size was not read.
- a named sparse vector with an on-disk index and the IDF modifier. The code comment says the live collections were switched to IDF by hand.
- `shard_number=6`, `replication_factor=2`, `on_disk_payload=True`, binary quantization held in RAM, HNSW in RAM, `default_segment_number=2`, `prevent_unoptimized=True`.
- strict mode on, with unindexed filtering refused for both reads and updates. A filter, a delete-by-filter or a `set_payload`-by-filter on a payload key with no index is rejected.

The vector name is the second `/`-separated segment of the model name, or the whole name when there is no `/` (`encoders/base.py:17-26`): `text-embedding-3-large` and `bm25`.

Payload indexes are listed in `vector_search/constants.py`: `QDRANT_LEARNING_RESOURCE_INDEXES` (25 keys), `QDRANT_CONTENT_FILE_INDEXES` (`key`, `title`, `platform.code`, `offered_by.code`, `file_extension`, `run_readable_id`, `resource_readable_id`, `edx_module_id`, `url`) and `QDRANT_TOPIC_INDEXES` (`name`).

## Point ids

Every id except a topic's is `str(uuid.uuid5(uuid.NAMESPACE_DNS, key))` (`vector_search/utils.py:333-343`). The key depends on the kind of point (`vector_point_key`, `utils.py:1532-1568`), where `platform` is the resource's `platform.code` or an empty string:

| Point | Collection | Key |
|---|---|---|
| learning resource | `resources` | `{platform}.{readable_id}` |
| content file chunk | `content_files` | `{platform}.{resource_readable_id}.{run_readable_id}.{key}.{chunk_number}` |
| resource metadata chunk | `content_files` | `{platform}.{readable_id}.course_information.{chunk_number}` |

`run_readable_id` is an empty string in the key when the content file has no run. The payload gets the resource's `readable_id` as a fallback `run_readable_id`, the key does not (`utils.py:598-616`).

A topic's point id is its `LearningResourceTopic.topic_uuid`, a random UUID stored in Learn's database (`utils.py:395`). The platform has it as `learningresourcetopic_uuid` in `stg__mitlearn__app__postgres__learning_resources_learningresourcetopic`.

## What gets embedded

### Learning resources

One vector per resource. The text is built by `_learning_resource_embedding_context` (`utils.py:534-588`):

1. The resource's metadata document rendered as markdown by `LearningResourceMetadataDisplaySerializer.render_markdown()` (`learning_resources/serializers.py:1200`), under the heading `# Information about this <resource_type>:`. This is the text of the resource drawer: title, description, instructors, prices, dates, levels, topics. For a program it includes child courses read from the database at render time.
2. A `**Course numbers:**` section.
3. `## Content` followed by the `content` of every content file attached directly to the resource (not the files of its runs).

The result is truncated to the model's `max_input_tokens` as LiteLLM reports it, counted with the model's tiktoken encoding (`encoders/utils.py:53-112`). When the limit or the encoding can't be found it cuts at 20,000 characters instead. There is no chunking.

Re-embedding is gated by a checksum: `md5("{RESOURCE_EMBEDDING_VERSION}\n{text}")`, with `RESOURCE_EMBEDDING_VERSION = 1`, stored in the payload as `embedding_checksum` (`utils.py:772-808`). When the stored value matches, only the payload is overwritten.

### Content files

The text is the content file's `content` field. Files with empty content get no points.

- A file with `file_type == "marketing_page"` or `file_extension == ".md"` is split on markdown headers (levels 1 to 4, headers kept), then by the recursive splitter. A chunk that lost its heading gets the header path prepended (`utils.py:441-489`).
- Everything else goes straight to `RecursiveCharacterTextSplitter.from_tiktoken_encoder(encoding_name="cl100k_base", chunk_size=512, chunk_overlap=51)` (`utils.py:411-431`).

Empty chunks are dropped but keep their index, so `chunk_number` can have gaps. Chunks are sent to the embeddings API 527 at a time (`min(2048, int(300000 * 0.9 / 512))`, `utils.py:966-972`).

Re-embedding is gated on the payload `checksum` of chunk 0 against the content file's `checksum` (md5 of the extracted text). On a mismatch every point for the file is deleted by filter on `key`, `resource_readable_id`, `platform.code` and, when the file has one, `run_readable_id`, then the new chunks are upserted. On a match the payload of every chunk of the file is still rewritten with `set_payload` (`update_content_file_payload`, `utils.py:720-741`).

Summaries are part of the same pass. Before chunking, `ContentSummarizer` (an LLM call) fills or rewrites `summary` and `flashcards` on the content file when the run uses summaries, and the refreshed values go into the payload (`utils.py:1080-1115`, `1195-1220`).

### Resource metadata chunks

`_embed_course_metadata_as_contentfile` (`utils.py:869-943`) writes each resource's metadata document into `content_files` as well. It is split with `RecursiveJsonSplitter(max_chunk_size=2048)` (4 characters per token times 512) and each fragment is rendered back to markdown under the same heading. The payload has `key = "{platform}.{readable_id}.course_metadata"`, `file_type = "course_metadata"`, `file_extension = ".txt"`, and `run_readable_id` set to the resource's `readable_id`. These chunks have their own gate: md5 of `str(render_document())` against the chunk-0 `checksum`, then a delete by `key` (`utils.py:886-904`).

### Topics

`embed_topics` (`utils.py:346-408`) embeds each topic's `name` and stores `{"name": name}`. It diffs topic names against the collection, embeds new ones and deletes the ones that are gone.

### Sparse vectors

With the deployed encoder, Learn sends `models.Document(text=<same text as the dense vector>, model="qdrant/bm25")` and the client is built with `cloud_inference=True` (`encoders/qdrant_cloud.py:36-49`, `utils.py:98-112`). Qdrant computes the sparse vector. The encoder also passes `options={"openai-api-key": settings.OPENAI_API_KEY}` on every document, including BM25 ones; the platform should not copy that. Resource, metadata-chunk and topic points are written with both vectors or, when the dense vector is all zeros, with neither (`points_generator`, `utils.py:161-166`). Content-file chunks always carry both.

## Payloads

### `resources`

The payload is the whole search document: the output of `serialize_learning_resource_for_bulk` (`learning_resources_search/serializers.py:842`), which is `LearningResourceSerializer` plus the search-only fields, plus `embedding_checksum`. It carries Learn's database ids, nested runs, topics, departments, instructors, images, `views`, `featured_rank`, `completeness`, `resource_age_date` and the attached content files.

Learn serves search hits from this payload without a database read. `VECTOR_SEARCH_RESOURCES_FROM_PAYLOAD` defaults to `True` (`main/settings.py:939`) and the Pulumi program does not override it. So the payload is the API response for a search hit, minus the keys in `RESOURCES_PAYLOAD_EXCLUDE` and a few fields trimmed in Python (`utils.py:1330-1367`).

`featured_rank` is rewritten daily by filter (`update_featured_ranks`), outside the embedding path.

### `content_files`

Per chunk (`utils.py:1039-1050`): `resource_point_id` (the id of the parent resource's point), `chunk_number`, `chunk_content`, and whichever of these the serialized content file has: `key`, `course_number`, `platform`, `offered_by`, `file_extension`, `content_feature_type`, `run_readable_id`, `resource_readable_id`, `run_title`, `edx_module_id`, `content_type`, `description`, `title`, `url`, `file_type`, `summary`, `flashcards`, `checksum`. `platform` and `offered_by` are the serializer's objects, filtered on as `platform.code` and `offered_by.code`.

### `topics`

`{"name": <topic name>}`.

## When Learn embeds

- On write: with the plugin hooks on, upserting or unpublishing a resource queues `generate_embeddings` or `remove_embeddings` (`learning_resources_search/plugins.py`).
- After a run's content files load: `content_files_loaded` (`plugins.py:276-305`) queues `remove_unpublished_run_content_files` and then `embed_run_content_files` (`vector_search/tasks.py:474-578`). That task compares each file's checksum and the fields in `CONTENT_FILE_PREPASS_PAYLOAD_FIELDS` (title, description, url, file_type, file_extension, content_type, edx_module_id, summary, flashcards) with the chunk-0 payload, embeds only the files that differ, and removes the points of files that no longer have content.
- Marketing pages: `marketing_page_for_resources` (`learning_resources/tasks.py:947`) scrapes a resource's page, stores it as a run-less content file (`file_type` `marketing_page`, extension `.md`, `key` the URL) and embeds it with `overwrite=True`. The platform's content-file models have no such rows.
- `embed_new_learning_resources` and `embed_new_content_files`: every 120 minutes, for rows created in the last 180 minutes, `overwrite=False` (existing points are skipped).
- `sync_topics`: 06:00, 18:00 and 23:00 UTC.
- `embeddings_healthcheck`: Saturdays 06:00 UTC, reports resources and content files with no points to Sentry.
- `tune_qdrant_collections`: 10:00 UTC daily, adjusts optimizer thresholds to the point count.

`generate_embeddings` is rate limited to 200 tasks a minute and retries Qdrant errors three times with backoff up to 10 minutes.

Which rows get points: resources that are published or in test mode, and published content files of every run of those resources, not only the best run (`qdrant_content_files`, `utils.py:1294-1304`).

Deletion is not symmetric with publication. When a run of a source in `QDRANT_RETAINED_SOURCES` (mit_edx, mitxonline, xpro, oll, canvas; `learning_resources/etl/constants.py:105-111`) is unpublished, its content-file points stay (`plugins.py:224-246`). They go when the run or the resource is deleted. A platform writer that deletes what is absent from the current models would remove points Learn keeps.

Three management commands also write: `generate_embeddings` (full or by-id re-embed, optionally recreating the collections), `create_qdrant_collections` and `sync_topic_embeddings`.

## What reads the points

- The vector search API, from the payload as described above.
- OpenSearch indexing copies each resource's dense vector out of the `resources` collection, by `readable_id` filter and dense vector name (`learning_resources_search/indexing_api.py:382-395`).
- `get_similar_topics_qdrant` and `get_similar_resources_qdrant` (`learning_resources_search/api.py:1032`, `1215`) look a resource's point up by its id.

A second writer or a separately named collection has to keep these working.

## What this means for the platform assets

The task descriptions for Cohort 5 were written in June against a simpler picture. Four things differ.

1. The `resources` payload cannot be produced from the `integrations__learn__*` models. It is the output of Learn's DRF serializers, keyed by Learn's database ids, and it is what the search API returns. A platform asset that upserts whole points into `<base>.resources` would have to reproduce those serializers, and any drift would show up as wrong API responses.
2. The dense text for a resource is also serializer output (the metadata markdown), so the platform cannot compute a vector that matches `embedding_checksum` without the same rendering.
3. Sparse vectors need no platform code. Sending the text as a `Document` to a client with cloud inference on is the whole of it.
4. Content-file embedding is interleaved with LLM summarization, which writes back to Learn's database.

The content-file points are the tractable part. `integrations__learn__content_files` and `integrations__learn__ocw_content_files` carry `key`, `run_readable_id`, `checksum`, `content` and most of the payload fields, so the chunking and most of the payload can be reproduced. They cover mitxonline, xpro and OCW only; the collection also holds mit_edx, oll, canvas and marketing-page files. For mitxonline and xpro OLX blocks the model's text approximates Tika's (the `content` column description in `_integrations__learn__cohort4__schema.yml`), so for those files point ids will match Learn's but checksums and chunks will not. They do not carry `resource_readable_id` or the platform code, and the point id needs both, so the embedding asset has to join each file to its run's resource in the catalog models (that join is not written yet). Also missing: `summary`, `flashcards`, `run_title`, `course_number`, `content_feature_type` (the OCW model has `content_tags`) and the `platform` / `offered_by` objects.

## Open decisions

The first four need an answer from the MIT Learn developers before the assets are built. The fifth is a lookup.

1. Same collections or separate ones. I recommend separate collections for the first build (e.g. `<base>.platform_content_files`), because the live ones serve search and strict mode, binary quantization and the hand-applied IDF change make a wrong write hard to notice and hard to undo. Point ids should still be computed with Learn's keys so the two can be compared id by id (ids and payload fields; chunk text will differ where the extracted text does). Moving to the live names is then the cutover, the same way an `ETLSourceOwnership` row is for catalog sources.
2. Who writes the `resources` collection. Three options: Learn keeps it (the platform only takes over content files and topics); the platform writes vectors and Learn writes payloads (two writers on one point, ordered by `embedding_checksum`); or Learn stops serving hits from the payload so the payload can shrink to filter fields the platform has. I recommend the first until content files have been cut over. It costs nothing now and the second and third both need Learn changes.
3. Where summaries and flashcards are generated once the platform writes content-file points: stay in Learn and be pulled into the payload, or move with the embedding.
4. Whether the platform should store vectors in Iceberg as well as Qdrant. It would let a collection be rebuilt, or a second one filled, without paying for the embeddings again. `docs/design/adr_embedding_compute_strategy.md` (status: proposed) proposes the same for feedback embeddings (an Iceberg `ARRAY<float>` column).
5. Whether the dense vector size in the live collections is 3072. One `get_collection` call answers it.
