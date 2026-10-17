# Courseware knowledge as platform data: concepts, outcomes, prerequisites and assessments in the dimensional layer

Status: proposed. Date: 2026-10-16. Related: `embedding_substrate_and_search_engine.md` (branch `embedding-substrate-spec`), `adr_embedding_compute_strategy.md`, hq RFC #13220 (answer separation), hq RFC #13314 (learner memory), hq RFC #11522 (Omnigraph).

Sources read on 2026-10-16: this repo at `07eadd990`, mit-learn at `d2eef5677`, learn-ai `main` at `df18361`, the course-profile repo at its single commit (2026-10-15), the "Enhancing course context for hybrid search" document, and the ontology and tooling sources linked in section 6.

## 1. The problem, and what is already in flight

MIT Learn search embeds a short markdown rendering of each resource (title, description, topics, price, instructors, runs) plus a concatenation of its content files. A query like "courses where students implement a neural network" has nothing to match against except whatever phrasing happens to appear in that text. Learn has no structured record of what a course teaches, assumes, or assesses: no concept, skill, outcome or prerequisite field exists on `LearningResource`, `LearningResourceRun` or `ContentFile`. The nearest things are free-text CMS sections folded into `full_description`, `CredentialMetadata.criteria` (LLM-written badge criteria, one skill per bullet), and `ContentFile.summary`/`flashcards`. The topic taxonomy is two levels deep (11 parents, 101 children) and is applied per resource, never per unit, video or problem.

Two efforts are addressing this, and they do not share a destination:

- "Enhancing course context for hybrid search" proposes a map-reduce LLM pipeline inside Learn: extract topics, concepts, algorithms, outcomes, prerequisites and `relationships` triples per content file (MAP), consolidate and deduplicate (REDUCE), then render one markdown "context document" per course for the existing embedding path (SYNTHESIS). The structured JSON is explicitly a transient step; the deliverable is the markdown, treated as a derived search artifact.
- course-profile (https://github.com/mitodl/course-profile) is a standalone prototype that profiles one OCW course, one OLX export, or one captioned video. Its unit of output is a _claim_ (concept, skill, assumed knowledge, assessment expectation) with a `teaching_status` of taught, reviewed, used or mentioned, and every claim carries a verbatim passage that is re-verified against the extracted text. It keeps declared prerequisites, inferred preparation and sequencing gaps apart, and applies a code rule that excludes any inferred prerequisite the course teaches before first use. Four runs exist (8.01SC, 6.0001, UAI.4, one 18.06 lecture); 8.01SC produced 659 claims from 61 Opus calls (515k input, 222k output tokens, about $6 to $8 at high effort). Output lands in JSON files and a SQLite viewer on Vercel and Fly.

Both re-extract text the platform already holds, both key their output to their own identifiers rather than ours, and neither produces something a second consumer (AskTIM, the product page, a knowledge graph, a prerequisite report, a B2B skills view) can read without re-running the pipeline. Both are also upstream of two decisions this repo has already made: AI compute is engine-external and its outputs are Iceberg tables (`adr_embedding_compute_strategy.md`), and the text that gets embedded is platform data, with the serving store a rebuildable projection (`embedding_substrate_and_search_engine.md`).

## 2. What the platform already has

The structure is done. The text is mostly done. The semantic layer is absent.

| layer | what exists | where |
|---|---|---|
| Catalog and structure | `dim_course`, `dim_course_run` (SCD2), `dim_program` (with `program_what_you_learn`, `program_prerequisites`), `dim_course_content` (one row per block per retrieval, `is_latest`), `dim_problem`, `dim_video`, `dim_discussion_topic`, `dim_ocw_course`, `dim_ocw_resource`, `dim_topic`, `dim_instructor`, `dim_department`, `bridge_course_topic`, `bridge_program_course` | `src/ol_dbt/models/dimensional/` |
| Block bodies, problem XML included | `int__combined__course_xml_blocks.coursestructure_xml_raw_xml` for edxorg, mitx, mitxonline, xpro. 782 problems in one 14.310x export. Nothing in the dimensional layer reads it. | `src/ol_dbt/models/intermediate/combined/` |
| Document and transcript text | `course_document_text` (Tika), `course_transcript_text` (SRT, SJSON, VTT), `course_file_exclusions` (staff-only, unreferenced) for mitx, mitxonline, xpro; `raw__ocw__s3__course_content` for OCW pages, resources and transcripts; `raw__youtube__api__transcripts` (no staging model yet) | `dg_projects/openedx/openedx/assets/{content_files,transcripts,content_file_exclusions}.py`, `src/ol_dlt/ol_dlt/sources/ocw_content/` |
| Learn-shaped content files | `integrations__learn__content_files` (mitxonline, xpro), `integrations__learn__ocw_content_files`, keyed by `(etl_source, run_readable_id, key)` | `src/ol_dbt/models/integrations/learn/` |
| Learn taxonomy | `seeds/learn_topics.csv` (112 rows with uuids) and `learn_topic_offeror_mappings.csv` (540), generated from Learn's production tables as replicated to the warehouse (not `topics.yaml`) and kept equal to them by `tests/assert_learn_topic_seeds_match_mitlearn.sql` | `src/ol_dbt/seeds/` |
| Free-text outcomes and prerequisites | `int__mitxonline__courses.course_what_you_learn` / `course_prerequisites`, `stg__edxorg__api__course.course_prerequisites_text`, OCW syllabus pages | intermediate, staging |
| LLM plumbing | `LLMClientFactory` (anthropic, bedrock, gemini, openai), Opik prompt library and tracing, `PolarsIcebergIOManager` upsert, embedding clients with model-versioned upsert keys | `dg_projects/ml/ml/resources/llm.py`, `ml/lib/embed.py` |
| An LLM-proposed vocabulary precedent | `dim_feedback_category`: LLM-proposed categories with `category_status` and `category_source`. Every category stays `proposed` and is used as soon as it exists; a person corrects it afterward, and nothing waits on approval | `docs/design/feedback_dimensional_model.md` |

Gaps that matter here: edxorg has block XML but no document or transcript extraction; Canvas has archives but no text at all; `dim_problem` has `markdown` but no parsed question, choices or solution; there is no `dim_content_file`, so text is not joinable to `dim_course_content` by `content_block_fk`; `bridge_course_topic` omits OCW and edxorg; `dim_topic` is per-platform names and does not carry the Learn uuids. Those are prerequisites for this work, not part of it, and most are already tracked under the Learn ETL migration project.

## 3. Options

Three shapes, with the same extraction quality assumed in each.

Learn-side pipeline (the context document proposal). Learn's Celery workers run MAP/REDUCE/SYNTHESIS over `ContentFile` rows and write the markdown back for embedding. Cheapest path to a better embedding; the structured JSON is discarded or stored as a blob on the resource. The cost is that it lives in the application that has the least of the inputs (no edxorg text, no block structure, no tracking-log signal about which problems are actually graded) and the fewest consumers (search only). A second consumer, or a second platform, means a second pipeline, and the platform would later ingest Learn's blob back out to analyze it. It also runs the extraction over problem text before #13220 has split questions from answers.

Standalone profiler (course-profile as a service). Keep the prototype's pipeline and viewer, point it at our buckets, publish its JSON. Best extraction design of the three today, and its reviewer UI is real. But its identifiers are its own (`U03:c2`), its store is SQLite, its ingesters re-derive course structure from OCW HTML and OLX trees that we already have as tables, and its outputs are not joinable to enrollments, grades or engagement without a second integration. It would become a fourth place courseware text is extracted (Learn, openedx assets, ocw_content, course-profile).

Platform assets in the dimensional layer (proposed). Extraction runs as Dagster assets over the text the platform already holds, keyed by `content_block_pk` and `courserun_readable_id`, and writes claims, evidence, entities and relationships into Iceberg as `dim_`, `bridge_` and `tfact_` models. Learn consumes rollups through the integrations schema exactly as it consumes courses and content files today; the context document becomes a renderer over those tables and one `builder_id` in the embedding substrate; a knowledge graph is a projection of the edge tables when a consumer needs traversal. The cost is that it is the slowest route to a better embedding (the schema has to exist first), and it puts subject-matter review of extracted claims on a team that does not own the courses.

I propose the third. The extraction is expensive enough (thousands of courses, tens of LLM calls each) that it should be paid once into a store every consumer can read, and the two existing efforts already agree on the content of what to extract. What they disagree on is only where the state lives, and that is the platform's question to answer. The course-profile claim model is the right extraction contract and should be adopted as is; the context document is the right first consumer and should be built as a view.

## 4. The model

Sections 4 to 6 are the proposal as first written. Where they differ from what the proof of concept built, sections 9 to 11 are current: the built table names (section 9), concept identity and resolution (section 10, "Identity" and "Batched, incremental resolution") and the concept-to-concept relation types (section 10, "Relationships between concepts").

Grain and keys follow `src/ol_dbt/models/dimensional/README.md`: surrogate `*_pk` via `generate_surrogate_key`, every course join carries `platform`, `platform` plus `courserun_readable_id` identifies a run. The Open edX XML and text assets record the deployment instead (`xpro`, `mitx`), so `int__courseware__extraction_sources` maps those to `mitxpro` and `residential` before it joins them to `dim_course_content`.

### Claims and evidence (facts)

`tfact_courseware_claim`: one row per extracted assertion. This is course-profile's `Claim` with platform keys.

| column | notes |
|---|---|
| `claim_pk` | hash(extraction_run_fk, claim_source_id) |
| `platform`, `courserun_fk`, `courserun_readable_id` | |
| `scope_block_fk` | the `dim_course_content` block the claim is about: a sequential for unit-level claims, a problem or video for item-level ones. OCW: the `dim_ocw_resource` or page, via the same key space once `dim_content_file` exists |
| `claim_kind` | `concept`, `skill`, `assumed_knowledge`, `assessment_expectation`, `learning_outcome`, `declared_prerequisite`, `audience` |
| `label`, `description` | the model's canonical label and one or two sentences, as extracted and never edited |
| `teaching_status` | `taught`, `reviewed`, `used`, `mentioned` (concept and skill kinds) |
| `prerequisite_kind` | `prerequisite`, `corequisite`, `recommended`, `explicitly_none` (declared prerequisites) |
| `flagged_as_prior` | assumed knowledge framed as prior by the material ("recall", "from 18.01") |
| `bloom_process`, `knowledge_type` | learning outcomes only, from the Bloom-revised SKOS scheme in section 6 |
| `concept_fk` | null until resolution assigns one (section 5) |
| `extraction_run_fk` | |

`tfact_courseware_claim_evidence`: one row per quoted passage. `claim_fk`, `content_file_fk` or `content_block_fk`, `passage` (verbatim), `locator` (timestamp, page, heading), `char_start`, `char_end` against the platform's own text, and `verification_status` in `exact`, `fuzzy`, `unverified`, `unknown_source`. The verification is recomputed against `integrations__learn__content_files.content` (or its OCW and edxorg equivalents), so a claim's evidence is checkable from the warehouse, not from a cache on someone's laptop.

`dim_extraction_run`: `extraction_run_pk`, `extractor_id`, `model_id`, `prompt_version`, `schema_version`, `input_text_checksum`, `course_xml_version`, `started_at`, `token_in`, `token_out`, `dagster_run_id`. Same convention as `embedding_vectors.model_id`: a model or prompt change adds rows and never overwrites, so two extractor versions can be compared on the same course.

Assessment claims from Open edX problems are extracted from the question side only. #13220's split (prompt text vs. solution and correct-answer markup) is a dependency: until it lands, problem blocks are extracted with `<solution>` and `correct="true"` content stripped by the capa-aware extractor the RFC specifies, and no evidence passage is quoted from the solution side. The claim "this problem assesses eigendecomposition" is publishable; the answer is not.

### Entities (dimensions)

`dim_concept`: the resolved, cross-course entity. `concept_pk`, `preferred_label`, `definition`, `concept_kind` (`concept`, `method`, `skill`, `tool`, `application`), `status` (`proposed`, `accepted`, `merged_into`, `rejected`), `merged_into_concept_fk`, `wikidata_qid`, `created_by_run_fk`, `reviewed_by`, `reviewed_at`. The status column follows the `dim_feedback_category` pattern, but the gate is new: feedback categories are used while still `proposed`, whereas here the extractor proposes, a person accepts, and only accepted concepts reach Learn-facing views. A `skill` is a concept phrased as an ability; the kind column keeps them queryable together for a knowledge graph and apart for a skills report.

`dim_concept_alias`: `concept_fk`, `alias`, `alias_source` (`extracted`, `wikidata`, `esco`, `openalex`, `acm_ccs`, `physh`, `manual`). This is the label index that resolution retrieves against.

`bridge_concept_vocabulary`: `concept_fk`, `vocabulary` (`learn_topic`, `openalex_subfield`, `openalex_topic`, `esco_skill`, `acm_ccs`, `physh`, `msc2020`, `onet`, `cip`), `external_id`, `match_type` (`exact`, `close`, `broad`), `match_source`. The crosswalk, in SKOS terms. `learn_topic` rows point at the uuids already in `seeds/learn_topics.csv`, so every concept rolls up to the facet Learn's UI already filters on.

`bridge_concept_relation`: `from_concept_fk`, `to_concept_fk`, `relation` (`prerequisite_of`, `broader`, `related`, `uses`, `part_of`), `support_count`, `first_run_fk`, `status`. Concept-to-concept edges are derived (the same concept `used` in a course that teaches a second concept, across enough courses), and they are the only table a graph projection needs beyond the course bridges.

`dim_learning_outcome`: `outcome_pk`, `statement`, `bloom_process`, `knowledge_type`, `status`, with `bridge_learning_outcome_concept` for the concepts an outcome names. Outcomes stay distinct from concepts because an outcome is a sentence about a learner and a concept is a thing; schema.org collapses both into `teaches`, and we need the distinction for Bloom-level reporting and for the badge criteria Learn already generates.

### Course-level rollups (bridges)

Derived by dbt from the claims, filtered to accepted concepts and verified evidence:

- `bridge_courserun_concept`: `courserun_fk`, `concept_fk`, `relation` (`teaches`, `reviews`, `uses`, `mentions`, `assesses`, `requires`), `first_block_fk`, `claim_count`, `max_confidence`. `requires` is the union of declared prerequisites and inferred preparation that survived the prerequisite rule (a concept assumed before any unit teaches it), with `requirement_source` recording which.
- `bridge_course_concept`: the run bridge collapsed to the course, for catalog consumers.
- `bridge_courserun_learning_outcome`, `bridge_problem_concept` (`problem_block_fk`, `concept_fk`, from assessment claims; joins to `tfact_problem_events` for "which concepts do learners fail on"), `bridge_video_concept`.
- `bridge_program_concept`: the course bridge summed over `bridge_program_course`, which gives the program-level skills list that `CredentialMetadata` currently asks an LLM to write from scratch.

### Serving

- `integrations__learn__resource_knowledge`: one row per `(etl_source, readable_id)` with arrays of accepted concepts (label, kind, relation, Learn topic uuid, wikidata_qid), outcomes with Bloom level, prerequisites with source (declared vs. inferred) and the claim counts behind each. Pulled by Learn's existing warehouse ETL task pattern (`BaseWarehouseETLTask`), under the ownership and shadow-run machinery it already has.
- `integrations__learn__resource_context_document`: the SYNTHESIS output, rendered deterministically from the tables above (title, topics, concepts by relation, outcomes, prerequisites, applications, assessments). It is `doc_kind = resource_context`, `builder_id = resource_knowledge_markdown_v1` in `embedding_documents`, so it is embedded, evaluated and promoted through the substrate like any other arm. An LLM pass over the rendered document is a second builder, if the evaluation shows the prose matters.
- schema.org on Learn product pages: `teaches`, `assesses`, `coursePrerequisites` and `educationalAlignment` from the same view. Learn has `FAQPage`, `VideoObject` and `PodcastEpisode` JSON-LD today and no `Course` markup.
- ol-analytics-api: a `course_knowledge` tenant over StarRocks views of the concept graph, for traversal queries (prerequisite paths, courses that teach a concept, next courses, neighbours). Section 10 covers why these go there and not through Learn.
- learn-ai: structured prerequisites and concepts for the recommendation bot (it currently reranks by level in the prompt because the catalog has no level filter) and for #13314's enrollment phase ("you have already covered X"). Per-course fields arrive through Learn's search API; traversal tools call the tenant.
- Graph projection: `bridge_concept_relation` plus the course and problem bridges exported as triples when a traversal consumer exists. Omnigraph (#11522) or anything else; the edge tables are the system of record either way.

## 5. The pipeline

Steps 3 and 4 changed in the proof of concept: extraction is one call per scope (section 9), and resolution is the batched resolver in section 10.

Dagster assets in `dg_projects/ml` (the LLM resources and the Iceberg IO manager are there), partitioned by `(platform, courserun_readable_id)` like the openedx content assets, with `code_version` tied to the prompt and schema versions.

1. Scope assembly (dbt). One row per unit with its ordered text: the sequential's blocks for Open edX, the week or lecture page group for OCW (`int__ocw__pages` plus the resources that page links), the chapter boundaries course-profile derives for standalone videos. The unit, not the block, is the extraction scope, because telling "taught here" from "used here" needs the surrounding lesson; blocks are the evidence locator. The platform's `course_file_exclusions` and the answer split are applied before any text reaches a model.
2. Mention propagation (CPU, deterministic). Aho-Corasick (`ahocorasick_rs` or `pyahocorasick`) over normalized text, built from `dim_concept_alias` and the vocabularies' labels, tags every block with the concepts it mentions, so a concept found by the LLM in one course is located in every course. Written to a `concept_mention` table with spans. Model-based pre-tagging (GLiNER2) is optional and only pays off if it lets the LLM step classify candidates or skip blocks; section 9 has the measurements.
3. Claim extraction (LLM). The course-profile prompts and Pydantic schemas (`course_profile/prompts.py`, `schemas.py`), ported as the first `extractor_id`, run through `LLMClientFactory` with prompts in Opik. Three calls per course plus one per unit, as the prototype does. Structured output via forced tool use, with the schema generated from the LinkML model in section 6 so the warehouse contract and the LLM contract are one file. Evidence passages are verified against the platform text before the write. Model tier is a per-run parameter; the prototype used Opus at high effort and recorded 28 to 61 calls per course.
4. Resolution (corpus-wide, not per document). Each claim label is embedded, retrieved against `dim_concept_alias` and a local Wikidata label index (never live WDQS), reranked by a small LLM call, and either attached to an existing concept, proposed as a new one, or queued. Low-confidence merges go to review. The review surface is Superset over the claim and evidence tables for the first pilot; course-profile's viewer, pointed at Iceberg instead of SQLite, is the candidate for a real reviewer UI since it already does claim-to-passage highlighting.
5. Rollups and rules (dbt). The prerequisite rule (`validate.py:apply_prereq_rule` in the prototype) becomes a dbt model: an assumed-knowledge claim is a `requires` edge only when no unit at or before first use has a `taught` or `reviewed` claim for the same concept. Doing it in SQL over resolved `concept_fk` is more reliable than the prototype's label matching, and it is testable with dbt unit tests.
6. Incremental. Re-extract a unit when its `input_text_checksum` changes (the `course_xml_version` the exclusions asset already carries), re-resolve when an alias table changes, re-render the context document on either.

Cost shape. At the prototype's usage (about 500k input and 200k output tokens per course on Opus), 3,011 Learn courses is roughly 1.5 billion input and 0.6 billion output tokens for a full backfill. Which tier runs which step is a pilot question; the claim and evidence tables make a cheaper extractor directly comparable to the expensive one on the same courses, which is the point of keying everything by `extraction_run_fk`.

## 6. Vocabularies and ontologies

Concept identity at the end of this section is superseded by section 10: a concept is an opaque UUID, and a Wikidata QID is linked to it (fast-follow 8 in section 11) rather than being its id.

The structure (course, instance, module, learning object, video, problem) already exists as tables, so the ontology question is only about the semantic layer: which property and relationship vocabulary to emit, which controlled vocabularies to align to, and what to use as concept identity. Findings, checked against the sources on 2026-10-16:

CEDS Ontology (https://github.com/CEDStandards/CEDS-Ontology, Apache-2.0, v14 released 2026-03-11, pushed 2026-09-28). 243k triples, 406 entity classes, 2,336 properties, 19,546 option-set concepts, all `schema:domainIncludes` with no OWL object or datatype property declarations: a data dictionary exported as RDF. `Course` has 24 properties and none for prerequisites, outcomes or topics. `LearningResource` mirrors LRMI. `CompetencyDefinition` is CASE-like with a single literal prerequisite identifier and only the three Bloom domains. Oriented to K-12 and state and federal reporting (SCED, EDFacts, Perkins). Not a backbone; a crosswalk if we ever report to such a system.

schema.org Course, CourseInstance, LearningResource and LRMI (v30.1, 2026-09-15). `Course` has `coursePrerequisites` (AlignmentObject, Course or Text), `syllabusSections`, `hasCourseInstance`; `teaches` and `assesses` are core, apply to any CreativeWork or LearningResource, and range over `DefinedTerm` or text; `educationalAlignment` gives framework-anchored alignment; `competencyRequired`, `educationalLevel`, `Quiz` and `LearningResource` itself are still pending. No learning-outcome class, no concept-to-concept prerequisite, no provenance or confidence. It is the right _published_ layer: Learn, Google and aggregators understand it, and it needs only a small extension.

EducOnto / educ-ontokg (https://nicolas-hbt.github.io/educ-ontokg/educonto/, ISWC 2022, last commit 2022-09-10). 71 classes and 29 object properties modeling French secondary-to-university track recommendation (Cpge, Atc, `requiresHighSchoolSpecialty`, `likedCurriculum`). Nothing on course content, outcomes or assessment. Related work only.

OntoCast (https://github.com/growgraph/ontocast, Apache-2.0 with CLA, 0.6.9 on 2026-10-14, 137 of 141 commits by one author). Docling chunking, then a LangGraph render-and-critique loop that emits ontology patches per chunk, SHACL-checks them, then extracts facts in those terms; RDF 1.2 output with span provenance into Fuseki or memory, Qdrant or LanceDB for ontology retrieval. `RENDER_MODE=facts` with a fixed ontology turns evolution off. Entity disambiguation is per document; there is no corpus-level resolution and no external KB linking; documents run sequentially with several LLM calls per chunk. Worth a bounded spike on 20 courses to draft the `mitol:` extension terms from what the model keeps wanting to add. Not the production pipeline: RDF out, high cost per chunk, one maintainer, and resolution would still be ours to do.

1EdTech CASE 1.1 (final 2025-01-24): `CFItem` competencies with associations `isChildOf`, `isPeerOf`, `isPartOf`, `exactMatchOf`, `precedes`, `isRelatedTo`, `replacedBy`. The edge vocabulary to borrow and the export format for a competency framework. Credential Engine CTDL has `ceterms:teaches`, `targetCompetency`, `requires`, `isPreparationFor` and runs its own LLM catalog extraction (https://github.com/CredentialEngine/ctdl-xtra, pushed 2026-10-14), which is prior art and an export target for MITx Online and xPRO credentials. IEEE LOM is paywalled and superseded in practice by LRMI. The syllabus and outcome ontologies in the literature (CCSO and others) are unmaintained.

Controlled vocabularies, in order of how much we should lean on them:

- MIT Learn topics (112, uuids in `seeds/learn_topics.csv`): mandatory rollup, because it is the facet the UI already has.
- OpenAlex (CC0): 4 domains, 26 fields, 252 subfields, 4,516 topics; subfields carry Wikidata QIDs; concepts are deprecated. The general academic subject backbone, from the free snapshot (the API is now metered). Topics are research-paper clusters, so some map loosely to teaching concepts.
- Discipline schemes: ACM CCS 2012 (SKOS, about 2,000 concepts, CS only), PhySH (CC0, about 3,000, pushed 2026-03-24), MSC2020 (63/529/6,022 classes, CC BY-NC-SA, license needs checking before anything derived is redistributed).
- Skills: ESCO v1.2 (13,939 skills, SKOS and JSON-LD, free, EU wording, weak on academic STEM); O*NET 31.0 (CC BY 4.0, occupation bridge, 35 generic skills); Lightcast Open Skills (about 33k skills, free only under a nonprofit license with approval and a 50 requests a month extraction cap, otherwise commercial).
- CIP 2020 at course and program level; ISCED-F only for international reporting.
- Bloom revised (6 processes, 19 sub-processes, 4 knowledge types): no maintained OWL exists, so a small SKOS scheme of our own in `seeds/`.

Concept identity: Wikidata QIDs where they exist (CC0, broadest coverage, local dump index), with `mitol:` ids minted for teaching concepts Wikidata lacks. Crosswalk coverage inside Wikidata is thin (486 items with an ACM CCS code, 408 with an MSC id, 1,195 of 13,939 ESCO skills), so the crosswalk table in section 4 is ours to populate, not something to read off Wikidata.

Schema language: LinkML (Apache-2.0, 1.12.1 on 2026-10-13). One YAML schema generates the Pydantic models the extractor validates against, the JSON Schema the LLM is forced to emit, the SQL DDL and dbt contract columns, and OWL, SHACL and a JSON-LD context for the graph projection. `slot_uri` mappings to schema.org and LRMI terms give the published vocabulary; the `mitol:` extension holds LearningOutcome as a node, concept-level `requiresConcept`, and assertion provenance (block, span, extractor, model, prompt version, confidence, run). This is how the warehouse contract and the extraction contract stay one artifact.

Tooling by stage: Aho-Corasick (`ahocorasick_rs`, Apache-2.0; `pyahocorasick`, BSD-3) for dictionary propagation, spaCy 3.8 (MIT) for sentence splitting and noun chunks; GLiNER2 2.1 (fastino, Apache-2.0 code and checkpoints) or GLiNER 0.2.29 (only the versioned `-v2.1` checkpoints are Apache; the unversioned `gliner_base`, `gliner_medium` and `gliner_large` are CC-BY-NC) for zero-shot mentions. Not YAKE: its repository LICENSE is AGPL-3.0, whatever PyPI says. Not KeyBERT: slowest and lowest recall in the benchmark. scispaCy only for a medicine and biology side job, and its PyPI metadata pins Python below 3.13. For the LLM step, instructor or forced tool use with LinkML-generated Pydantic, or Google LangExtract 1.7 (Apache-2.0) when character-span grounding matters more than schema depth; OntoGPT/SPIRES (BSD-3, 1.2.0 on 2026-09-09) is the reference for "the model emits names, the pipeline grounds them" and is worth reading, not adopting, since its grounding is biomedical. No maintained off-the-shelf Wikidata linker exists (ReFinED's last push was 2024-12 on a 2022 snapshot, ReLiK needs Python below 3.12, BLINK is archived), which is why resolution in section 5 is retrieval over our own alias index plus an LLM rerank. DSPy earns its place once a gold set of outcome, prerequisite and concept annotations exists.

## 7. Phasing

0. Schema and fixtures, no LLM spend. Write the LinkML schema and the dbt models in section 4. Load the four existing course-profile runs (8.01SC, 6.0001, UAI.4, 18.06 lecture 1; 1,884 claims) into the claim, evidence and concept tables as the first extraction runs, re-verify their passages against the platform's own text where the course exists in the lake, and run the prerequisite rule in SQL against them. This shakes out the model on real output before any pipeline exists and gives a regression baseline for the ported extractor. Prerequisite work from section 2 that this phase needs: `dim_content_file` keyed to `dim_course_content`, and OCW content keyed into the same space.
1. Pilot extractor. Port course-profile's prompts and schemas as `extractor_id = course_profile_v1` in `dg_projects/ml`, run it over 20 courses (10 OCW, 10 MITx Online and xPRO, chosen to overlap the embedding substrate's first eval set), with candidate mentions and resolution against Learn topics, OpenAlex and Wikidata. Compare against the prototype's output on the courses both have. Review in Superset; decide on the reviewer UI. Run the OntoCast spike on the same 20 courses and diff its emergent terms against the LinkML schema.
2. Serving. `integrations__learn__resource_knowledge` and the rendered context document as a substrate builder; evaluate the context-document arm against `learn_v1` on the substrate's eval set. Learn adds the warehouse pull and, if the arm wins, points `resources` embedding at it. Concepts as a Learn search facet is Learn's call at this point.
3. Backfill and incremental. Full corpus in the cheapest tier that held up in the pilot, with a second extractor run on a sample for comparison; schema.org `Course` markup on product pages; `bridge_program_concept` replacing LLM-written badge criteria; the `course_knowledge` tenant in ol-analytics-api, and AskTIM tools that call it for prerequisites and next courses.
4. Graph engine. Project `bridge_concept_relation` and the course bridges into an engine when a consumer needs k-hop expansion with ranking or graph algorithms at request time, which the tenant's SQL over precomputed tables doesn't give. Not before.

We do _not_ need, yet: a graph database, a competency framework export (CASE or CTDL), Lightcast, per-learner concept mastery (that is `bridge_problem_concept` joined to `tfact_problem_events`, and it falls out once the bridge exists), or an LLM in the SYNTHESIS step.

## 8. Open questions

- Answer separation (#13220) is a dependency for assessment claims from Open edX problems. Blocking for the problem part of phase 1, not for the rest.
- Extraction scope for OCW courses that are a flat list of resources with no week structure: course-profile's lecture-layout heuristics (syllabus calendar to attach problem sets) would need to become dbt logic over `int__ocw__pages`. Blocking for OCW in phase 1.
- Who reviews concept merges and claim quality. The feedback pipeline's category review is a precedent, but these claims need subject knowledge. Blocking for promoting anything past `proposed`.
- Whether Learn wants concepts and outcomes as facets, as product-page content, or only inside the embedded document. Changes the shape of the integrations view, not the dimensional model. Non-blocking.
- Auth for the `course_knowledge` tenant: anonymous read-only behind APISIX with a rate limit, or a client-credentials client per consumer (learn-ai, the Learn frontend). Blocking for the tenant, not for ingest.
- StarRocks latency for chat-time tool calls. Measure p95 for the tenant's routes in QA before AskTIM depends on them. Blocking for the AskTIM tools.
- Model tier per step, and whether the per-unit extraction can be batched through a provider batch API. Non-blocking; the pilot answers it.
- MSC2020 license terms for derived crosswalks. Non-blocking; drop MSC if unclear.

## 9. Local proof of concept (2026-10-16)

The pipeline in section 5 ran end to end against production courseware text through the local DuckDB lake (`ol-dbt local register`), for three MITx Online runs that form a sequence: UAI.2 (Python Coding, Part 2), UAI.3 (Foundations of Data Analytics and Machine Learning) and UAI.4 (Supervised and Unsupervised Learning), all `1T2026`. UAI.4 is the course mit-learn#4095/#4111 and course-profiles.fly.dev use, so the outputs are directly comparable.

What it consists of:

- `int__courseware__extraction_sources` (dbt). Scopes from `dim_course_content`: one per sequential with its html, problem and video blocks in course order, plus a context scope (about pages, reachable tabs) and a materials scope (static documents). Text comes from the OLX blocks, Tika document text and English transcripts already in the lake. Problems go through `courseware_problem_question_text`, which removes solutions, hints and feedback elements and every attribute that holds an answer. A `library_content` slot contributes its first variant.
- `ml.lib.courseware` (dg_projects/ml). One call per scope, run in parallel, because the course header (title, outline, description) comes from the warehouse and not from an earlier model call. The claim contract follows course-profile's model plus learning outcomes with Bloom process and knowledge type. The model calls a `record_extraction` tool, and its input is validated with Pydantic and retried once. Every passage is verified against the exact text sent, with character offsets. Output is three normalized tables keyed by `extraction_key` (a hash of extractor, prompt and schema version, model, effort and the full message), so a re-run skips stored scopes and a prompt change adds rows beside the old ones. The first version of `resolve` sent every distinct label to one call that proposed merges and broader links; section 10 replaces it.
- dbt over those outputs: `int__courseware__claims`, `_concept_mentions`, `_concept_labels`, `_concept_keys`; `dim_courseware_extraction`, `tfact_courseware_claim`, `tfact_courseware_claim_evidence`, `dim_concept`, `dim_concept_alias`, `bridge_concept_relation`, `bridge_courserun_concept` (with the prerequisite rule in SQL); and `integrations__learn__resource_knowledge`, with the context document rendered by SQL.

Measured, on `us.anthropic.claude-opus-5-5` at medium effort over Bedrock:

| | UAI.2 | UAI.3 | UAI.4 |
|---|---|---|---|
| scopes (calls) | 10 | 15 | 17 |
| claims | 385 | 439 | 523 |
| input / output tokens | 173k / 53k | 145k / 69k | 222k / 82k |
| cost at first-party list price | $1.59 | $1.74 | $2.38 |

Wall clock was 2 to 3 minutes per course at six concurrent calls. Of 1,949 evidence passages across both prompt versions, 1,928 (98.9%) matched the source exactly and the rest were near matches; none were unverified. One scope failed once because the model added an undeclared key, and validation now ignores extra keys. The first, single-call resolution merged 106 of 456 label keys into 48 concepts (e.g. recall, sensitivity and true positive rate) and proposed 61 broader links (e.g. Gini impurity under decision tree), giving 398 concepts, 53 of them in more than one course. Some of its merges were wrong ("random module" with "random number generation").

Findings:

- The cross-course join is the result neither in-flight effort can produce. UAI.4's inferred requirements resolve to concepts UAI.2 teaches (pandas, Matplotlib, DataFrame) and UAI.3 teaches (standard deviation, KDE, train/validation/test split), with no per-course configuration. Python and scikit-learn remain unmatched, presumably because UAI.1 was not extracted.
- Label normalization alone is not resolution. Before the resolution step, "AUC", "AUC / ROC curve" and "logarithm"/"logarithms" each produced a false requirement. Some proposed merges are wrong ("random module" with "random number generation"), so `proposed` status and review matter.
- Prompt v1 produced course-internal pointers ("concepts from Lecture 3") as assumed knowledge and "Lectures 2-4" as a declared prerequisite. v2 forbids both, and both versions' rows stay queryable side by side.
- The Tika text of an html body keeps no hrefs, so a slide deck cannot be placed in the sequential that links it, which is why the materials scope exists. Recording `/static/` references in the document-text asset would fix it.
- Problems inside `library_content` join to the OLX export by file name, not by the staged `coursestructure_xml_block_id` (the inner `url_name`). That should be corrected in staging.
- The rendered context document is too long: every taught concept, near-duplicate outcomes. Ranking and capping are SQL changes with no re-extraction.
- mit-learn#4111's unit prompt invites "answers and explanations when the platform provides them" into the model's input. The SQL macro here removes them before any text leaves the warehouse.

Reproducing it:

```bash
ol-dbt local register --all-layers
(cd src/ol_dbt && dbt run --target dev_local --select int__courseware__extraction_sources \
  --vars '{courseware_courseruns: ["course-v1:UAI_SOURCE+UAI.4+1T2026"]}')
duckdb ~/.ol-dbt/local.duckdb -c "copy main_intermediate.int__courseware__extraction_sources to 'sources.parquet'"
cd dg_projects/ml && .venv/bin/python -m ml.lib.courseware.local extract ../../sources.parquet out/ --dry-run
```

Then register `out/*.parquet` as `glue__ol_warehouse_production_intermediate__<table>` views, build `+int__courseware__concept_labels`, export it, run `ml.lib.courseware.local resolve`, register the two resolution tables and build `int__courseware__claims+`.

### Haiku 5.5 vs. Opus 5.5 on the same scopes

UAI.4 was extracted a second time with `us.anthropic.claude-haiku-5-5`, using the same prompt, scopes and effort, and written to a separate output so it does not become `is_current`. Labels from both models were resolved together before comparing, because resolution built from one model's labels counts the other's variants ("SHAP" vs. "SHAP values") as misses.

| | Opus 5.5 | Haiku 5.5 |
|---|---|---|
| claims | 523 | 572 |
| evidence exact / fuzzy / unverified | 537 / 7 / 0 | 580 / 6 / 2 |
| distinct concepts (concept + assumed) | 147 | 152 |
| taught concepts | 100 | 101 |
| input / output tokens | 222k / 82k | 222k / 198k |
| cost at first-party list price | $2.38 | about $0.12 |

Input tokens include 25k cache writes and 45k cache reads in both runs. Costs use Anthropic's published prices, checked 2026-10-17: Opus 5.5 at $4 input, $5 cache write, $0.20 cache read and $20 output per million tokens; Haiku 5.5 at $0.10, $0.125, $0.01 and $0.50 (every call's prompt was under the 100k tokens where Haiku's higher tier starts). The runs went through Bedrock, whose prices can differ.

Haiku found 82% of the concepts Opus marked as taught (and Opus 78% of Haiku's overall). Where both named a concept in the same scope, they agreed on teaching status 88% of the time; most disagreements are taught vs. reviewed and used vs. reviewed. What Opus has and Haiku lacks is mostly peripheral ("Markdown", "survey data", "distance matrix"). Haiku produced more assessment claims (70 vs. 42) and fewer stated learning outcomes (34 vs. 44). Neither model is ground truth here. A small gold set annotated by people is what decides between them, and it is also what DSPy or prompt tuning would need. On this evidence, Haiku is the bulk extractor for a backfill. Opus runs on a sample to keep a second extractor's rows next to it for drift checks, and wherever the claims feed a decision (prerequisites on a product page).

### CPU-bound concept tagging

Benchmarked on UAI.4 (185 blocks, 399k characters) against the Opus 5.5 run's 173 concept labels, on an i7-13800H under WSL2 with torch on 6 threads. 52 of the 173 labels never appear verbatim in the text ("choosing the number of clusters", "interpretability–accuracy trade-off"). So no extractive method can find more than 70% of them, and recall below is also given against the 121 labels that do appear.

| method | chars/s | recall of text-present labels | share of distinct spans matching a label |
|---|---|---|---|
| Aho-Corasick over the UAI.4 labels | 15.9M to 17.3M | 1.00 (by construction) | n/a |
| spaCy noun chunks | 78k | 0.90 | about 2% |
| GLiNER2-base, threshold 0.3 | 1.6k | 0.91 | about 7% |
| GLiNER-medium-v2.1, threshold 0.3 | 1.2k | 0.78 | about 12% |
| YAKE | 86k | 0.79 | about 2% |
| KeyBERT (MiniLM) | 1.9k | 0.47 | about 1.5% |

On a 120-chunk sample, GLiNER-large-v2.1 found 0.75 of the text-present labels at 0.7k chars/s, below GLiNER2-base (0.84 on the same sample). int8 quantization of the stock GLiNER checkpoint returned no spans. The span-match share understates precision, because many unmatched spans are real concepts the LLM did not list ("computer vision", "ROC curve").

The useful CPU job is propagation, not discovery. The UAI.4 dictionary, matched over UAI.3 and UAI.2 (courses whose text it was not built from), tagged 47% and 56% of their blocks, including real mentions those courses' own extractions left out. A 200k-pattern automaton builds in under a second and still matches at 5.7M chars/s, which puts the whole corpus (about 4.4 GB of text, an estimate) at minutes on one core. GLiNER2 at this speed would take about a month of single-machine time, so it is for discovery on new material at most. Two caveats:
- Matching tells us a concept is mentioned, not whether it is taught. Teaching status still needs the LLM, or a classifier trained on its labels.
- Short or generic aliases ("R", "recall", "kernel") need a case-sensitive pass, a minimum alias length, and per-alias ambiguity flags.

No maintained Wikidata linker fits this. ReFinED, REL, BLINK and spacy-entity-linker are stale, and ReLiK has no weight license. GLinker (Knowledgator, Apache-2.0, 2026) is new and unbenchmarked. Linking belongs at the concept level instead: a few thousand `dim_concept` rows resolved once against a local Wikidata label and alias dump, with an embedding or LLM rerank. Mentions then inherit the QID through the dictionary.

So for the "Haiku 5.5 for NER" question, the split is:
- Haiku 5.5 does the reading and judgment (claims with teaching status, about $0.12 a course).
- Aho-Corasick does the corpus-wide locating of known concepts (effectively free).
- The resolution and description steps keep the dictionary clean.

### Learner-facing concept descriptions

Claim descriptions are scope-specific by design ("used here to compare two models"), so they are evidence, not display text. `ml.lib.courseware.describe` writes one `short_description` (at most 12 words) and one `description` (one or two course-agnostic sentences) per resolved concept, plus an `is_learner_facing` flag. It reads up to five claim descriptions from different course runs (`int__courseware__concept_description_inputs`), using the course titles to choose the sense of an ambiguous label. Haiku 5.5 described all 398 concepts in 10 batched calls. 27 were flagged as not learner-facing, mostly datasets and case studies (Bluebikes, Framingham, the wine prices dataset), plus a few debatable ones ("docstring", "factorial"). A concept is described again only when its inputs' checksum or the prompt or model changes. `dim_concept` now carries `short_description`, `description`, `is_learner_facing` and `description_model_id`, and keeps the claim text as `extracted_description`. `integrations__learn__concepts` is the Learn-facing list, filtered to learner-facing concepts with teaching-course counts, for chips, tooltips and a concept page. The descriptions are `proposed` like the concepts, and they are the obvious first thing for a reviewer to edit.

### Graph projection

The edge tables stay the system of record and the graph is an export of them. One dbt model per node label and per edge type (`integrations__graph__node__concept`, `integrations__graph__edge__courserun_concept`, ...), because that is the shape every bulk loader takes. A thin union view of nodes and edges serves QA and RDF generation.

- Ids reuse the `_pk` keys (`course_pk`, `courserun_pk`, `concept_pk`, `program_pk`, `topic_pk`). The exception is `content_block_pk`, which hashes `block_id` with `retrieved_at` and so changes on every structure retrieval. Block and unit nodes key on a hash of `platform`, `courserun_readable_id` and `block_id` instead, the key `dim_course_content` holds unique for its latest rows, because the same block can exist on two platforms.
- Edges: PART_OF (block, unit, chapter, run), RUN_OF, IN_PROGRAM, the six run-to-concept relations with `claim_count` and `requirement_source`, ASSESSES from problems, BROADER, PREREQUISITE_OF and RELATED between concepts, ALIGNS_TO vocabulary terms with SKOS match type, and IN_TOPIC.
- Every edge carries `status` and its source row's key. Evidence passages stay in `tfact_courseware_claim_evidence` and are joined by key when a citation is needed. The review state lives in the warehouse, so engine-side branching would duplicate it.
- Two exports from the same models: `published` (accepted concepts, verified evidence) for Learn, the tutor and JSON-LD, and `review` with everything. Learner edges (enrollment, attempts, a derived per-concept mastery) come later, as a separate export under a separate access policy.
- A Dagster asset writes Parquet per label to S3 per snapshot, plus CSV for loaders that need it and N-Triples via the LinkML context for an RDF store. The graph is rebuilt per snapshot (low millions of edges without learners) and swapped by pointer. Only learner edges would need incremental upserts.

Plain SQL over the bridges already answers "what should I take before X" (two joins), structural and topic rollups, and coverage. Prerequisite chains should be a materialized closure table built by the NetworKit graph asset (section 10), which also fails on a cycle as a data-quality check. Trino's recursive CTE is experimental with a fixed depth, and StarRocks needs a flag with a default depth of 5, so the serving tenant in section 10 reads the closure table and never recurses at request time. An engine earns its place only for low-latency k-hop expansion with ranking (GraphRAG context for learn-ai), constrained variable-length paths, graph algorithms and interactive exploration.

Engines, checked 2026-10-16:
- Kùzu was archived on 2025-10-10. LadybugDB is the active fork (MIT, near-weekly releases, Parquet `COPY FROM`, vector and full-text search), though one author wrote about two thirds of its commits.
- DuckPGQ (MIT, SQL/PGQ in DuckDB) is a single-maintainer research extension. It ran here unmodified over the bridge tables, matching courses that teach what UAI.4 requires (course → concept ← course) and bounded `broader` paths.
- Neo4j Community is GPLv3. The vector index is in Community, while the GDS Community edition is capped at 4 cores and 3 catalog models.
- Neptune Analytics is the managed option: in memory, openCypher, Parquet import, one fixed-dimension vector index per graph.
- PuppyGraph queries Iceberg in place but is proprietary and documents no vector search.
- Omnigraph is the team's agent-memory store. For this projection it lacks a Parquet loader, uses its own `.gq` language, bounds every traversal, and has had six minor releases since July. witan has already hit OCC and merge-semantics problems with it. It stays the agent-memory store.
- Ruled out: Apache AGE (not on RDS), Memgraph (BSL, memory cost at learner scale), FalkorDB (SSPL), ArangoDB (BSL with a non-commercial community build), NebulaGraph and JanusGraph (stale).
- For the schema.org and SPARQL side, Oxigraph embedded, or Jena Fuseki if SHACL or reasoning is needed.

Spike order: DuckPGQ first (no infrastructure, matches `ol-dbt` local development; stop if SQL/PGQ covers the queries), then LadybugDB as a read-only file Dagster builds and learn-ai opens in process, then Neo4j Community in Kubernetes as the server baseline, with Neptune Analytics as the managed comparison.

Not done yet: see the fast-follow list in section 11.

## 10. Concept identity, relationships and graph topology

A practice review against published research and industry systems (`courseware_knowledge_practice_review.md`) found extraction in line with consensus. It found two divergences in identity. Keys derived from labels churn whenever a label or a resolution run changes, which breaks joins. A single call over every label is the weakest entity-matching setup in the literature, and it stops fitting in one prompt long before 50k labels. Both are now replaced.

### Identity

A concept is an opaque UUID minted once by `ml.lib.courseware.resolve` and never derived from a label (the Wikidata, Reltio and Tamr pattern). Each resolution run writes full snapshots:
- `courseware_concept_registry`: every id ever minted, with its status (active, merged, deprecated) and a redirect from each merged id to its survivor.
- `courseware_concept_assignment`: which concept each normalized label belongs to.
- `courseware_concept_relation`: concept-to-concept proposals.

Every verdict goes to the append-only `courseware_resolution_decision`, and each run gets a row in `courseware_resolution_run` with its churn counts. dbt reads the newest run. `dim_concept.concept_pk` is the UUID itself, and retired concepts keep their rows with `merged_into_concept_fk`. The display label is chosen at read time from the most-mentioned member label, so renaming a concept never changes its id. Identity stays separate from display, as MDM survivorship does it.

### Batched, incremental resolution

Each run resolves only labels with no assignment:
1. Block each label by Titan v2 embedding nearest neighbours (label, kind and description) plus character-trigram Jaccard.
2. Send eight new labels per call, each with up to twelve candidates, to Haiku 5.5. It gives a SKOS-style verdict per candidate: exact, close, broader, narrower, related or distinct.
3. Merge only `exact` verdicts. Components are rejected if larger than six or if any member pair was judged distinct, so one bad link cannot chain unrelated labels. Close, broader and related become relations.
4. Decide `broader` direction by votes, since pairs judged from both sides sometimes disagree. Carried edges vote too; a tie becomes `related`.
5. `--rebuild` re-resolves everything and carries ids forward by member overlap (Jaccard at least 0.5).

Measured on the pilot:

| run | labels | adjudicated | calls | concepts minted | existing ids kept |
|---|---|---|---|---|---|
| first (empty state) | 456 | 456 | 57 | 415 | n/a |
| incremental, after adding UAI.1 and UAI.5 | 650 | 194 | 25 | 171 | 456 of 456 |
| full rebuild (trial copy) | 650 | 650 | 82 | | 636 of 650 (97.8%) |

The batched resolver merges more conservatively than the single call did (456 labels became 415 concepts rather than 398), and the wrong "random module" merge no longer happens. New labels joined existing concepts where they should: "random library" joined "random module", "mean squared error" joined "MSE", and "lists" joined "python list". A batch that fails leaves its labels unassigned, so the next run retries them rather than minting duplicates. The 14 assignments a rebuild changed are the model's verdicts varying between runs, which the overlap carry-forward contains. A rebuild should still be a reviewed event, not a scheduled one.

### Relationships between concepts

`int__courseware__concept_relation_evidence` records every observation that two concepts are related. `bridge_concept_relation` aggregates it into one edge per (from, to, relation), with the evidence kept as columns, so consumers choose thresholds and the edge table evolves as courses are added:

| family | relation | evidence | meaning |
|---|---|---|---|
| hierarchy | broader | adjudication | from is a kind or part of to |
| sequence | builds_on | course sequence | a unit teaching `from` relies on `to`, and `to` is taught earlier in the course or not at all |
| practice | practiced_with | course sequence | the same, when `to` is a tool: a practice dependency, not a step in learning order |
| equivalence | close | adjudication | nearly interchangeable (exact matches are merged into one concept instead) |
| association | complements | co-taught units, co-assessed items | taught or assessed together |
| association | related | adjudication | connected, neither the same nor a kind of the other |

Each edge has `courserun_support`; `builds_on` also has `reverse_courserun_support` and `direction_consistency`; co-taught `complements` has `co_taught_lift` (observed over expected); and every edge has `is_adjudicated`, `first_observed_at`, `last_observed_at` and `review_status`. Splitting out `practiced_with` mattered. Without it, "accuracy builds on scikit-learn" and "bar chart builds on Jupyter Notebook" dominated the sequence edges. Course evidence and adjudication don't overlap yet, because they produce different relation types. Agreement between them (an adjudicated `related` pair that is also co-taught) is a confidence signal still to add.

On the five-course pilot the table holds 8,161 edges over 586 concepts after thresholds.

Inferred prerequisites publish only with support beyond one model call (`is_published`, with `unpublished_reason` on the rest, which keep their rows for review). A `builds_on` edge needs two units, counting a course's reruns once because they have near-identical text, and at least 0.75 of the units showing the pair in either direction must agree (`direction_consistency` over `course_scope_support` and `reverse_scope_support`). An inferred `requires` in `bridge_courserun_concept` needs two units relying on the concept before the run teaches it, or two courses requiring it (declared, or inferred with a direction that clears the threshold), and the same 0.75 share against the units that teach it later. The thresholds are the `courseware_prerequisite_*` dbt vars. The `acyclic` test holds published `builds_on` to a strict partial order, and the graph job builds its sequence view from published edges only. On the pilot, published `builds_on` went from 2,485 edges to 65, inferred `requires` from 143 to 38, and builds_on cycle nodes from 3 to 0. Leiden found 12 communities instead of 8 (modularity 0.66 vs. 0.63), since the association view lost the unpublished `builds_on` edges too. Single-unit edges that are textbook-correct (logistic regression builds on linear regression, gradient boosting on decision tree) are among those held back until another course corroborates them. The rule is not yet evaluated against human labels; the gold set (item 1 in section 11) should stratify on these support columns.

### Graph topology

`ml.lib.courseware.graph` runs on NetworKit (MIT, 11.2.2 from 2026-09-15, cp314 wheels; parallel C++ with Leiden, PageRank, SCC and sparsification), which replaced networkx before anything was built on it. Its three views of the edge table are covered in the module docstring.

Outputs are per graph run:
- per-concept positions in `afact_concept_graph_position`;
- communities in `dim_concept_community`, whose ids carry forward across runs by member overlap like concept ids;
- edges with a local-degree backbone score in `courseware_concept_graph_edge`, so a client can draw a sparsified graph by threshold;
- run summaries in `afact_concept_graph_run` (density, sparsity, components, communities, modularity, cycle counts), whose history is how the graph's evolution is tracked.

| graph run | concepts | edges | sparsity | communities | modularity | nodes in builds_on cycles | nodes in broader cycles |
|---|---|---|---|---|---|---|---|
| 3 courses | 415 | 5,400 | 0.937 | 9 | 0.59 | 0 | 70 |
| 5 courses, direction voting | 586 | 8,161 | 0.952 | 8 | 0.63 | 3 | 6 |

Communities read as coherent topics: ML evaluation; Python plotting; chart types; ML theory; descriptive statistics; object-oriented Python; data management; clustering; and, after UAI.1 and UAI.5, Python fundamentals and neural networks. Six of the 3-course communities kept their ids in the 5-course run, two new ones appeared, and 299 of 415 concepts stayed in their community. The foundation rank puts normal distribution, Python programming and arithmetic mean at the top. The broader cycles came from contradictory directions; direction voting cut them from 70 nodes to 6. The cycle counts are now a monitored data-quality signal, with members to send to review.

### Serving: Learn ingest plus an ol-analytics-api tenant

Delivery to learners goes two ways, depending on where the query runs.

Learn ingests what has to sit beside its own search index. `integrations__learn__resource_knowledge` and `integrations__learn__concepts` arrive through the warehouse-pull ETL, keyed by concept UUID. They supply per-course concepts and prerequisites as search fields and an OpenSearch facet, the context document embedded into Qdrant, concept chips, and a concept detail endpoint. This path can't move to StarRocks, because our clusters are shared-data and StarRocks's vector index runs only on shared-nothing clusters (`embedding_substrate_and_search_engine.md`, on the unmerged `embedding-substrate-spec` branch).

Graph traversal goes to a `course_knowledge` tenant in ol-analytics-api. It reads StarRocks views built from the dimensional tables through the lake's external catalog:
- the prerequisite path for a course, declared vs. inferred, with the courses that teach each prerequisite (from the closure table);
- courses that teach a concept, by teaching status and support;
- concepts matching a learner's wording, through `dim_concept_alias`;
- a concept's neighbours, by relation family and community;
- next courses for a set of known concepts: courses whose required concepts the set covers, ranked by `builds_on` and community.

The tenant returns course keys (`etl_source`, `readable_id`), concept ids, labels and short descriptions. Price, availability and URLs stay in Learn, so callers fetch course details there. A merged concept id redirects to its survivor, so an id a caller cached keeps working. AskTIM's prerequisite, concept and next-course tools call the tenant (learn-ai already calls services other than Learn, e.g. Zendesk); Learn's concept pages and "courses to take first" do too.

The alternative was Learn copying the relation, closure and community tables into Postgres and rebuilding these queries in Django. That is a second copy of a contract that changes with every resolution and graph run, and every change to the graph schema becomes a Learn migration. ol-analytics-api already has what this tenant needs from the b2b tenants: StarRocks views over Iceberg, dbt exposures that hold a view's columns to the API's selects, and a committed OpenAPI spec per tenant. The TypeScript client pipeline for that spec isn't wired up yet. The data carries no learner PII. That matters because ol-analytics-api isolates tenants only in the application layer, and pods can be reached without going through the gateway. Those two limits constrain `b2b_learner_records` and are acceptable here.

What it costs: AskTIM tool calls make one more network hop and depend on one more service. Callers make a second lookup in Learn for course details. Data is as fresh as the dbt build plus the view refresh, which is fine for course content. Auth and chat-time latency are open (section 8).

## 11. Fast-follow work (accepted from the practice review)

1. Human gold set. 300 to 500 claims, stratified by course family and by model agreement (both models, one only), plus 20 to 40 exhaustively annotated subsections for recall, double-annotated in part, with precision estimated by stratified prediction-powered inference. Same-family model agreement overstates recall, because shared misses are invisible. A few hundred labelled concept pairs and about 100 sampled clusters for resolution, scored with B-cubed.
2. Claim-quote support check. Run a sampled NLI or LLM-judge check that each verified passage supports its claim, with a per-claim confidence built from match tier, cross-scope and cross-model agreement, and calibrated on the gold set.
3. Explicit promotion of extraction versions. Replace "newest extraction wins" (`is_current`) with a promoted extractor version per scope, so a trial run or a second model never becomes current by running last.
4. Support threshold for inferred prerequisites. `requires` and `builds_on` from a single claim let the model's known bias toward predicting prerequisites through. Require support across scopes or runs, enforce a strict partial order (no cycles) as a dbt test, and evaluate against sampled annotated pairs. Thresholds and the acyclicity test are done (section 10); evaluation waits on the gold set.
5. Course-header hashing. The outline and description are part of every extraction key, so editing a course's about page re-extracts the whole course. Key on the scope's own text and record the header version separately.
6. Review workflow. A queue over proposed concepts, merges, relations and descriptions: auto-accept only high-confidence exact merges, and keep reviewer decisions in their own table that the resolver reads as constraints, so review is not overwritten by a rerun.
7. Cross-signal relation confidence. Raise an edge's confidence when adjudication and course evidence agree, and track edge strength over time with a dbt snapshot of `bridge_concept_relation`.
8. Wikidata linking at the concept level. Link each concept, not each mention, against a local label and alias dump with an embedding or LLM rerank. Record QID redirects when Wikidata merges items.
9. Dagster assets. Wrap `extract_scopes`, `resolve_concepts`, `describe_concepts` and `compute_graph` as assets in `dg_projects/ml`, writing the same tables to Iceberg through the existing IO manager. Prompt versions go in Opik.
10. Dictionary propagation. Aho-Corasick tagging of every block with known concept aliases, with a case-sensitive pass and an ambiguity flag for short aliases, and a check that resolution errors are not multiplied across the corpus.
11. Scale paths. Use an ANN index (Qdrant or hnswlib) for blocking past tens of thousands of labels, and multithreaded Leiden with a fixed partition seed for large graphs.

## References

- course-profile: https://github.com/mitodl/course-profile (README, `docs/explanation-architecture.md`, `course_profile/schemas.py`, `course_profile/validate.py`), live at https://course-profiles.vercel.app and https://course-profiles.fly.dev
- "Enhancing course context for hybrid search": https://docs.google.com/document/d/1ghiaXPCWK-yBc4MnVgn6oZdphWVPcbNnSd23U7aiWh0
- ol-analytics-api: https://github.com/mitodl/ol-analytics-api (README, "Adding a new tenant" and "The published API contract"); this repo's `models/b2b_analytics/_b2b_analytics__exposures.yml` for the exposure pattern
- hq RFCs: #13220 answer separation, #13314 learner memory, #11522 agent memory layer (Omnigraph)
- This repo: `docs/design/embedding_substrate_and_search_engine.md`, `docs/design/adr_embedding_compute_strategy.md`, `docs/design/feedback_dimensional_model.md`, `docs/learn_marts_contract.md`, `src/ol_dbt/models/dimensional/README.md`
- Vocabularies: https://schema.org/Course, https://www.dublincore.org/specifications/lrmi/, https://github.com/CEDStandards/CEDS-Ontology, https://nicolas-hbt.github.io/educ-ontokg/educonto/, https://github.com/growgraph/ontocast, https://standards.1edtech.org/case/specifications/standards/v1p1/im, https://github.com/CredentialEngine/ctdl-xtra, https://help.openalex.org/data/topics/, https://esco.ec.europa.eu/, https://www.onetcenter.org/license_db.html, https://docs.lightcast.io/lightcast-api/docs/free-api-access, https://msc2020.org, https://github.com/physh-org/PhySH, https://linkml.io
- Tooling: https://spacy.io, https://github.com/urchade/GLiNER, https://github.com/google/langextract, https://github.com/monarch-initiative/ontogpt, https://github.com/567-labs/instructor
