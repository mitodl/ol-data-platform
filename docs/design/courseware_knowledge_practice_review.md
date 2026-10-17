# Extraction scales; concept identity needs rebuilding

Written on 2026-10-16 against the proof of concept as it stood before section 10 of `courseware_knowledge_extraction.md`. The label-derived keys (`int__courseware__concept_keys`) and the single resolution call it describes have since been replaced by opaque concept ids and batched resolution.

The extraction half of our courseware knowledge design matches or goes beyond published practice, and it will scale. It uses a closed claim schema, one parallel LLM call per subsection with warehouse-supplied course context, verbatim evidence quotes verified in code with character offsets, append-only rows keyed by a content hash, and warehouse tables as the system of record with the graph as a projection. On those points we sit with the schema-first, product-facing systems (OntoGPT/SPIRES, Google LangExtract, Apple's ODKE+, LinkedIn's Skills Graph). We are stricter than the open-source GraphRAG lineage on provenance and versioning ([Caufield et al., Bioinformatics 2024](https://academic.oup.com/bioinformatics/article/40/3/btae104/7612230); [LangExtract resolver](https://raw.githubusercontent.com/google/langextract/main/langextract/resolver.py); [ODKE+, arXiv 2509.04696](https://arxiv.org/abs/2509.04696)). Concept resolution is the part that will not scale. A single corpus-wide LLM call over every label is the setup the 2023-2026 entity-matching literature finds weakest. It also stops fitting in a context window long before 50k labels ([ComEM, COLING 2025](https://arxiv.org/html/2405.16884v3); [LLM-CER, arXiv 2506.02509](https://arxiv.org/html/2506.02509v1)). Deriving `concept_pk` from the canonical label contradicts every production identity system reviewed: Tamr, Reltio and Wikidata all mint opaque IDs once and redirect on merge ([Tamr](https://docs.tamr.com/new/docs/publishing-clusters); [Wikidata Help:Redirects](https://www.wikidata.org/wiki/Help:Redirects)). Two other gaps diverge from consensus. Our quote check shows that evidence exists but not that it supports the claim, and product KGs (Diffbot, ODKE+, Amazon COSMO) add a support check and per-fact confidence that we lack. And we have no human gold set, while Haiku-vs-Opus agreement is a same-family proxy that hides the misses both models share ([Kim et al., ICML 2025](https://arxiv.org/pdf/2506.07962)). The "used before taught" prerequisite rule is a sharper, deterministic version of ordering features that the literature has shown to work. Its main exposure is LLM positive bias, which the cross-course join amplifies ([Pan et al., ACL 2017](https://aclanthology.org/P17-1133/); [Yang et al., arXiv 2402.14293](https://arxiv.org/html/2402.14293)). Most fixes are cheap: opaque IDs with redirects, blocked candidate resolution, a sampled support check, and a few hundred human labels combined with model judgments through prediction-powered inference. A graph database is not justified at our scale.

## Verdict: maintainable and scalable once identity and evaluation are fixed

Extraction is maintainable as designed. Every model, prompt or schema change adds rows beside the old ones. We pay for that safety because hosted models drift and get retired: GPT-4's accuracy on one task fell from 84% to 51% between March and June 2023, OpenAI gives as little as 3 to 6 months' notice before retiring a model, and Azure auto-upgrades deployments ([Chen, Zaharia and Zou, arXiv 2307.09009](https://arxiv.org/abs/2307.09009v1); [OpenAI deprecations](https://developers.openai.com/docs/deprecations); [Azure OpenAI retirements](https://learn.microsoft.com:443/en-us/azure/ai-services/openai/concepts/model-retirements)). Extraction cost also scales: Haiku 5.5 ran UAI.4 for about $0.11 against $2.35 on Opus 5.5 ([design doc §9](courseware_knowledge_extraction.md)), and Aho-Corasick propagation runs at about 16M characters per second, so locating known concepts across the corpus takes minutes on one core.

The resolution and review layer is not yet maintainable. A rerun of resolution can rename a canonical label. Because `int__courseware__concept_keys` takes `concept_key` from the newest resolution's `canonical_label_key` and `dim_concept` hashes that key into `concept_pk` (`src/ol_dbt/models/intermediate/courseware/int__courseware__concept_keys.sql`, `src/ol_dbt/models/dimensional/dim_concept.sql:82`), a rename changes the key, which orphans every review decision and downstream join on it. The design doc's own section 4 already models `merged_into` status and `merged_into_concept_fk`, so the intent exists and the proof of concept has not caught up. No source quantifies review-backlog growth in a production LLM-built KG, so the review-throughput risk below is my inference, not a measured fact.

| Question | Our choice | Consensus | Verdict |
|---|---|---|---|
| 1. Pipeline shape and schema | Closed claim types, open concept labels, resolution as a later stage | Per-chunk extraction then separate canonicalization; schema-first where a product consumes the output | Aligned |
| 2. Entity resolution | One corpus-wide LLM call; key from canonical label | Blocking, then small-set LLM adjudication, then cluster repair; opaque IDs with redirects | Diverges; must change |
| 3. Grounding and confidence | Verbatim quotes, exact/fuzzy/unverified, char offsets; no confidence | Product KGs add a support check and per-fact confidence | Ahead on provenance, behind on support and confidence |
| 4. Prerequisites | SQL rule: relied on before or without being taught | Ordering features plus supervised or LLM pairwise classifiers, evaluated against small annotated samples | Aligned on signal; unevaluated |
| 5. Tiering and evaluation | Haiku bulk, Opus sample; no gold set | Cascades routed on confidence; small human sample plus PPI | Tiering aligned; evaluation diverges |
| 6. Operations | Append-only, content-hashed, `is_current` = newest; tables as system of record | HITL curation loop; versioned prompts; projection pattern at taxonomy scale | Aligned, with gaps in carry-forward and promotion |
| 7. Graph storage and serving | Parquet export, precomputed closure, engine spikes; hybrid-search context document | Graph DB only for online, deep traversal at billions of edges; hybrid retrieval beats GraphRAG on lookups | Aligned |

The changes with the highest impact for their effort, across all seven questions, are these. Mint opaque concept IDs with a redirect table and carry them forward by member overlap (Q2). Replace the single resolution call with embedding plus BM25 blocking and an LLM that classifies each candidate pair into SKOS relations (Q2). Build a 300 to 500 item human sample and estimate precision with stratified prediction-powered inference (Q5). Add a sampled claim-quote support check plus a derived corroboration confidence (Q3). Make promotion to "current" an explicit pointer instead of recency (Q6).

## 1. Every pipeline splits extraction from canonicalization, and a closed schema fits our consumers

### What practice and research say

The leading 2024-2026 pipelines share a shape: an LLM call per chunk, then a separate merge or canonicalization stage, with each item linked to its source chunk. They differ on schema. Microsoft GraphRAG defaults to four broad entity types, 1,200-token chunks and one gleaning pass. It merges entities only on identical title and type, and it leaves claim extraction off by default "because claim prompts really need user tuning" ([graphrag defaults.py](https://raw.githubusercontent.com/microsoft/graphrag/main/packages/graphrag/graphrag/config/defaults.py); [GraphRAG config docs](https://microsoft.github.io/graphrag/config/yaml/)). LightRAG, KGGen, iText2KG and EDC's extract step are open or lightly typed ([LightRAG, arXiv 2410.05779](https://arxiv.org/html/2410.05779v3); [KGGen, arXiv 2502.09956](https://arxiv.org/html/2502.09956v2)). OntoGPT/SPIRES, docling-graph, neo4j-graphrag with a user schema, LangExtract and ODKE+ are schema-first ([neo4j-graphrag KG builder](https://neo4j.com/docs/neo4j-graphrag-python/current/user_guide_kg_builder.html); [docling-graph](https://github.com/docling-project/docling-graph)).

No controlled head-to-head of closed schema against open IE exists for any domain with matched downstream tasks. The circumstantial evidence is consistent, though. Aligned to a target schema, EDC beat specialist baselines by 5 to 25 F1 points (WebNLG 0.820 vs. 0.767). Canonicalizing an open KG after the fact cut relation types from 529 to 200 at a small precision cost (0.982 to 0.956) ([EDC, EMNLP 2024](https://arxiv.org/html/2404.03868)). ODKE+, a fixed 195-predicate schema over 9M Wikipedia pages, reports 98.8% precision on 19M facts ([arXiv 2509.04696](https://arxiv.org/abs/2509.04696)). Open extraction tuned for RAG summaries does not give a usable standalone graph: in KGGen's manual check of 100 random triples, 98% of KGGen's were valid, 55% of OpenIE's and 0% of GraphRAG's ([KGGen](https://arxiv.org/html/2502.09956v2)). AutoSchemaKG shows open schema induction can recover 92% of a human schema over 50M documents. That is evidence open induction works at scale, not that it beats a curated schema for a defined product ([arXiv 2505.23628](https://arxiv.org/abs/2505.23628)). The October 2025 survey frames the choice as consistency against discovery and makes no recommendation ([Bian, arXiv 2510.20345](https://arxiv.org/abs/2510.20345)).

On granularity, GraphRAG's GPT-4 measurement found 600-token chunks yielded almost twice as many entity references as 2,400-token chunks. Gleaning recovers most of the loss ([Edge et al., arXiv 2404.16130](https://arxiv.org/html/2404.16130v2)). Prepending document context to each chunk cut retrieval failures by 35% to 67% in Anthropic's tests. That evidence is about retrieval. I found no study of document context for chunk-level _extraction_ ([Anthropic, Contextual Retrieval, Sept 2024](https://www.anthropic.com/engineering/contextual-retrieval)).

### How our design compares

We are schema-first for claim types and open for concept labels. That hybrid is common: it follows EDC and KGGen for entities and SPIRES and ODKE+ for predicates. It fits a fixed set of consumers (Learn facets, `requires` edges, schema.org `teaches`/`coursePrerequisites`, warehouse bridges) where cross-course comparability is what we sell. Open labels carry the consistency risk, so resolution (Q2) carries most of the quality burden for concepts. Using the subsection as the scope with a warehouse-built outline header follows the field's move from fixed token windows toward structural units, as in docling-graph and LightRAG's 2026 chunkers. Our scopes are large, though. UAI.4 averaged about 13k input tokens per call including the prompt and header (222k over 17 calls, [design doc §9](courseware_knowledge_extraction.md)), against 1,200-token defaults elsewhere, and we run no gleaning pass. The GraphRAG result predicts some recall loss on long transcripts. It is a 2024 GPT-4 entity-count result, so this is a judgment call. One more point concerns schema drift. The proof of concept now ignores undeclared keys after a scope failed on one. That keeps runs green, but it discards the signal OntoCast-style schema evolution would use.

### Recommended changes

| Rank | Change | Impact | Effort |
|---|---|---|---|
| 1 | Keep the closed claim schema. Log ignored extra keys and `mentioned`-only labels per run as a schema-gap report, and feed it into the planned OntoCast spike | Medium | Low |
| 2 | Measure claims per 1k tokens against scope length on the pilot's 20 courses; split scopes above a threshold or add one gleaning pass where yield falls | Medium | Low |
| 3 | Re-run 3 to 5 scopes twice on the same model to measure run-to-run claim stability, which no source has measured for closed vs. open extraction | Medium | Low |

## 2. One resolution call breaks before 50k labels, and label-derived keys break on rerun

### What practice and research say

LLMs now match fine-tuned matchers with few or no labels. The best LLMs need "no or only a few" training examples to match PLMs trained on thousands, and they generalize better to unseen entities ([Peeters, Steiner and Bizer, EDBT 2025](https://arxiv.org/abs/2310.11244v4)). How the question is framed matters more than which model answers it. With GPT-4o-mini over 8 datasets, pairwise yes/no matching scored 67.80 mean F1. Showing the LLM a blocked candidate set and asking it to select scored 82.26 at about a third of the cost. ComEM, which ranks 10 blocked candidates with a small model and lets the LLM choose among the top 4, scored 86.42 for $0.09, above supervised Ditto (80.69) ([Wang et al., COLING 2025](https://arxiv.org/html/2405.16884v3)). LLM-CER clusters small record sets directly. Quality peaks at about 9 records per set and drops beyond 9 to 10, and against pairwise matching it cut API calls 12 to 108 times ([Fu et al., arXiv 2506.02509, SIGMOD 2026](https://arxiv.org/html/2506.02509v1)). KGGen blocks with k-means into clusters of 128, shows the LLM the top 16 candidates by fused BM25 and embedding score, and resolved 4,602 entities to 3,573 for $0.38. Its authors list over- and under-deduplication as an open limitation ([KGGen, NeurIPS 2025](https://arxiv.org/html/2502.09956v2)). Deg-RAG found that at about 70% entity reduction "weakly related entities were also absorbed" ([arXiv 2510.14271](https://arxiv.org/html/2510.14271v1)). Ontology-matching work treats equivalence, subsumption and relatedness as distinct outcomes ([GenOM, arXiv 2508.10703](https://arxiv.org/pdf/2508.10703)). LinkedIn's KGBert labels skill pairs parent, child or no-relation. Its training negatives include siblings and loosely related pairs, and taxonomists make the final call on about 39k skills and 374k aliases ([LinkedIn Engineering, Mar 2023](https://www.linkedin.com/blog/engineering/data/building-maintaining-the-skills-taxonomy-that-powers-linkedins-skills-graph)).

Turning pairwise links into clusters by transitive closure introduces errors of its own. Graph-metric cluster repair with an LLM oracle beats earlier repair methods ([Christen et al., ACM JDIQ, June 2025](https://dbs.uni-leipzig.de/files/research/publications/2025-6/pdf/graph_metrics_driven_cluster_repair.pdf)). A global ILP over confidence-weighted edges resolved all 42 inconsistent components on MusicBrainz, and 98.1% of the edges it removed were true false positives ([Pires and Junior, SBBD 2026](https://sol.sbc.org.br/index.php/sbbd/article/view/44035)). Splink treats very large clusters as suspect and runs a separate cluster-QA stage ([Splink docs](https://moj-analytical-services.github.io/splink/topic_guides/evaluation/clusters/overview.html)).

On ID stability the production rule is the same everywhere. Mint an opaque ID once. On merge, one ID survives and the loser becomes a redirect that still resolves. On split, the largest piece keeps the ID. IDs are never reissued ([Tamr](https://docs.tamr.com/new/docs/publishing-clusters); [Reltio](https://docs.reltio.com/matchmerge/merginginreltio.html); [Wikidata Help:Redirects](https://www.wikidata.org/wiki/Help:Redirects)). Reltio computes the displayed label (survivorship) at read time, separately from identity ([Reltio survivorship](https://docs.reltio.com/en/model/consolidate-data/design-survivorship-rules)). Reclustering systems carry IDs forward by matching new clusters to old ones on shared members, and they score churn as a clustering diff ([van Staden, arXiv 2409.18254](https://arxiv.org/abs/2409.18254); [ABCDE, arXiv 2407.21430](https://www.arxiv.org/abs/2407.21430)).

### How our design compares

The proof of concept's single call over 456 labels produced 398 concepts, including a wrong merge of "random module" with "random number generation" ([design doc §9](courseware_knowledge_extraction.md)). That pair is _related_, not _equivalent_, which is exactly the case binary "same concept?" prompts handle worst and that sibling hard negatives and SKOS-style relation verdicts exist to catch. The single call does not scale either. 500k labels at about 5 tokens each is about 2.5M input tokens (my estimate), the output has to list every group, LLM-CER finds quality falls once a set passes about 10 items, and EDC keeps the full schema out of the prompt for the same context reason ([EDC](https://arxiv.org/pdf/2404.03868)). A different label set also yields different groups, so a rerun can rename a canonical label and change `concept_pk`. The design doc's section 5 already describes the right architecture (embed, retrieve against `dim_concept_alias`, rerank with a small LLM call, queue low-confidence cases), so the gap is between the proof of concept and the design. LLM cost is not the constraint. Using Steiner et al.'s January 2025 zero-shot rates, 5M candidate pairs (500k labels with 10 neighbors each) would cost roughly $55 on GPT-4o-mini or $1,000 on GPT-4o ([Steiner et al., arXiv 2409.08185](https://arxiv.org/html/2409.08185v2)). That is my extrapolation, and selection-style prompts cost less. Human review throughput is the real constraint.

### Recommended changes

| Rank | Change | Impact | Effort |
|---|---|---|---|
| 1 | Mint an opaque `concept_id` at first acceptance; make `dim_concept_alias` the lookup from normalized label to ID; make `preferred_label` a survivorship attribute | High | Low |
| 2 | Add a redirect/tombstone record (`status` in active/merged/deprecated/split, `replaced_by`, `changed_at`, `reason`); never delete or reuse an ID | High | Low |
| 3 | Replace the single call with: embed and BM25-index labels, block by Learn topic or OpenAlex field, take the top 10 to 16 neighbors, and have the LLM classify each pair as exact/close/broader/narrower/related/distinct | High | Medium |
| 4 | Resolve incrementally: new labels against the existing concept index only, not a full recluster | High | Medium |
| 5 | Cluster with repair (cut weak bridges with correlation clustering or ILP on small components), flag clusters above a size cap, and auto-accept only `exact` verdicts found from both directions | Medium | Medium |
| 6 | Gate each resolution run on ID churn (share of aliases whose ID changed) and sampled B-cubed precision via `er-evaluation` ([Binette et al., arXiv 2404.05622](https://arxiv.org/pdf/2404.05622)) | Medium | Low |

## 3. Quote verification proves evidence exists, not that it supports the claim

### What practice and research say

Three grounding approaches coexist. LangExtract asks for extractions and aligns them in code: exact match by monotonic DP, then fuzzy token LCS, accepted when at least 75% of tokens match. It keeps unaligned items with `char_interval = None` instead of dropping them ([resolver.py](https://raw.githubusercontent.com/google/langextract/main/langextract/resolver.py)). Anthropic's Citations API guarantees valid character, page or block pointers, but "citations and structured outputs are incompatible" ([Claude citations docs](https://platform.claude.com/docs/en/build-with-claude/citations)). Span models such as GLiNER return offsets by construction ([GLiNER, NAACL 2024](https://arxiv.org/abs/2311.08526)). All three show that evidence is _present_. None shows it _supports_ the claim. In ALCE, "even the best models lack complete citation support 50% of the time" ([Gao et al., EMNLP 2023](https://arxiv.org/abs/2305.14627)). Fine-tuned judges reach only about 80% macro-F1 on whether evidence supports a claim, with most errors on fine-grained detail ([AttributionBench, Findings ACL 2024](https://arxiv.org/pdf/2402.15089)). Up to 57% of citations were post-hoc rationalizations even when the cited document supported the statement ([Wallat et al., arXiv 2412.18004](https://arxiv.org/pdf/2412.18004)). In SPIRES, hallucinated entities were infrequent. Its dominant error was a wrong relation between real mentions, which a quote check cannot catch ([Bioinformatics 2024](https://academic.oup.com/bioinformatics/article/40/3/btae104/7612230)).

Product KGs close that gap with a second stage and a score. Diffbot keeps a 0 to 1 confidence on every fact, discards facts below 0.5, and raises confidence when reputable sources corroborate ([Diffbot docs](https://docs.diffbot.com/docs/confidence-score)). ODKE+ runs a Grounder LLM that validates each fact against its evidence before ingestion ([arXiv 2509.04696](https://arxiv.org/abs/2509.04696)). Amazon COSMO filters LLM assertions with critic classifiers trained on human annotations ([Amazon Science, SIGMOD 2024](https://www.amazon.science/publications/cosmo-a-large-scale-e-commerce-common-sense-knowledge-generation-and-serving-system-at-amazon)). Single-call confidence signals are weak. Logprob, verbalized and self-consistency baselines "collapse toward all-positive behaviour at practical thresholds", while fusing disagreement between two differently framed extraction calls reached 0.928 ROC AUC on DocILE ([ExtractConf, arXiv 2606.24420](https://arxiv.org/abs/2606.24420)). That is invoice field extraction, not educational claims, so the evidence here is thin.

### How our design compares

On provenance we are ahead of every open-source RAG-oriented tool reviewed. Those keep chunk IDs. We keep verbatim passages with offsets and a match tier recomputed against warehouse text, and our exact/fuzzy/unverified tiers mirror LangExtract's. 98.9% of 1,949 Opus passages matched exactly and none were unverified. Haiku had 2 unverified out of 588 ([design doc §9](courseware_knowledge_extraction.md)). At that rate, provider citations would buy little and would cost us the forced-tool-use schema. Programmatic verification is the right choice. The unmeasured risk is a real quote attached to the wrong `teaching_status`, Bloom level or concept. We keep no per-claim confidence. The design's `bridge_courserun_concept.max_confidence` and LinkML `confidence` slot have nothing to fill them. Measured against Diffbot, ODKE+ and COSMO, that is our main divergence from product consensus. Measured against open-source KG tools, it is normal.

### Recommended changes

| Rank | Change | Impact | Effort |
|---|---|---|---|
| 1 | Run a sampled support check on (claim, quote) pairs with a non-Claude NLI model or judge. Calibrate it on the human sample (Q5) before using it as a filter | High | Medium |
| 2 | Derive confidence from signals we already have: match tier, number of scopes and runs asserting the same (concept, kind, status), and Haiku/Opus agreement where both ran. Fill `max_confidence` from it | High | Low |
| 3 | Do not add a verbalized confidence field to the schema; the evidence says it will saturate | Low | None |
| 4 | Expose match tier and confidence in the integrations views so Learn can filter, as LangExtract consumers do | Medium | Low |

## 4. Our prerequisite rule is a sharper ordering feature, exposed to LLM positive bias

### What practice and research say

Academic benchmarks are small and only moderately agreed: Pan et al.'s three Coursera datasets have 128 to 244 concepts, 25% of pairs were sampled for labeling, and pairwise kappa was 0.59 to 0.65 ([Pan et al., ACL 2017](https://aclanthology.org/P17-1133/)). The large graphs are mostly model output. MOOCCubeX's roughly 1.267M prerequisite relations include only 4,865 human-annotated ones ([ACM CSUR survey, 2025](https://dl.acm.org/doi/10.1145/3733593)), and its graph covers "only a small portion" of the knowledge components in at least one slice ([arXiv 2506.22303](https://arxiv.org/html/2506.22303v1)). Ordering signals work. Pan's position, asymmetry and "complexity level" (coverage times survival across videos) features formed the strongest family, and removing complexity level alone cost 7.4 F1 points ([Pan et al.](https://aclanthology.org/P17-1133/)). Supervised results in 2026 reach F1 0.835 to 0.923 on the standard benchmarks. ProPRL also finds that GNNs drift toward learning relatedness rather than direction ([ProPRL, arXiv 2608.03006](https://arxiv.org/pdf/2608.03006)). Zero-shot GPT-4 reaches F1 0.777 on LectureBankCD with a documented positive bias, and chain-of-thought cut its CV F1 from 0.763 to 0.416 ([Yang et al., arXiv 2402.14293](https://arxiv.org/html/2402.14293)). Cross-domain transfer drops sharply, from 0.887 to 0.690 F1 in PRELEARN ([arXiv 2011.03760](https://arxiv.org/pdf/2011.03760)). Hypernym ("is-a") relations are confused with prerequisites often enough that Graphusion has a dedicated conflict-resolution step ([Graphusion, arXiv 2407.10794](https://arxiv.org/html/2407.10794v1)).

Evaluation follows one protocol: 2 to 3 domain annotators label a sample of pairs as a→b, b→a or none, and results are reported as P/R/F1 with kappa near 0.6. Strict partial-order checks (irreflexive, transitive) catch structural errors ([AL-CPL, EAAI 2018](https://clgiles.ist.psu.edu/pubs/eaai18.pdf)). A separate EDM line infers or validates prerequisites from learner performance ([Chen et al., EDM 2015](https://www.educationaldatamining.org/EDM2015/uploads/papers/paper_42.pdf); [COMMAND, EDM 2016](https://www.educationaldatamining.org/EDM2016/proceedings/paper_89.pdf)). I found no study that checks an LLM-built prerequisite graph against learner outcomes. Bloom tagging is mature: BERT reaches kappa 0.93 and F1 0.95 on learning objectives ([EDM 2022](https://educationaldatamining.org/edm2022/proceedings/2022.EDM-short-papers.55/index.html)), while zero-shot LLMs score 0.72 to 0.73 accuracy on outcomes and exam questions ([arXiv 2511.10903](https://www.arxiv.org/pdf/2511.10903)). Industry graphs (LinkedIn, Lightcast, Coursera) model hierarchy and relatedness, not prerequisites. Khan Academy removed its knowledge map ([Khan Academy support](https://support.khanacademy.org/hc/en-us/community/posts/360027982751-What-happened-to-the-knowledge-map)).

### How our design compares

Our rule is close to Pan's Average Position Distance, but it uses LLM-assigned `taught`/`used` status instead of raw mention position. That fixes the main weakness of mention ordering, a concept mentioned in passing early on. I found no published method with an explicit per-unit teaching status. That is a search result, not a verified absence. Our course→concept→course join also avoids the quadratic pair space that forces sampling in pairwise classifiers. As implemented, though, one assumed-knowledge or `used` claim before first teaching produces a `requires` edge (`src/ol_dbt/models/dimensional/bridge_courserun_concept.sql:84-108`). It has no support threshold, so LLM positive bias flows straight into edges, and every spurious requirement can then match some course that teaches it. Our Bloom labels come from a zero-shot LLM, the regime with the weakest published accuracy.

### Recommended changes

| Rank | Change | Impact | Effort |
|---|---|---|---|
| 1 | Annotate a sample of inferred `requires` edges plus an equal number of non-edges with 2 to 3 instructors or IDs; report kappa and precision | High | Medium |
| 2 | Add support to inferred edges (claim count, number of scopes, Pan-style coverage-times-survival) and publish only edges above a threshold; keep single-claim edges in the review export | High | Low |
| 3 | Keep `broader` and `requires` mutually exclusive per pair, flag conflicts for review, and fail on cycles in the closure asset (already planned) | Medium | Low |
| 4 | Use prerequisites explanatorily first ("this course assumes X; these courses teach X"), not for gating | Medium | Low |
| 5 | Double-code a few hundred outcomes for Bloom process (six-way and collapsed lower/higher) and compare against the 0.9 kappa range | Medium | Low |
| 6 | Validate a sample of cross-course edges against problem attempts (learners who completed the teaching course do better on X-mapped problems), controlling for learner ability | Medium | High |

## 5. Haiku's 21x saving holds up, but agreement with Opus is not accuracy

### What practice and research say

Cascades report large savings: up to 98% cost reduction at GPT-4 quality for FrugalGPT, over 2x for RouteLLM, and up to 90% for online cascade learning, where small models learn from LLM labels collected during the stream ([FrugalGPT, arXiv 2305.05176](https://arxiv.org/abs/2305.05176); [RouteLLM, arXiv 2406.18665](https://arxiv.org/abs/2406.18665); [Online Cascade Learning, ICML 2024](https://arxiv.org/abs/2402.04513)). All of these are classification or QA benchmarks with gold labels. I found no cascade study on open-ended structured extraction. LinkedIn's skill extraction is the closest industry precedent. It runs a trie tagger over the taxonomy, a two-tower BERT tagger for paraphrases, and a distilled encoder 80% smaller for CPU serving, and it is evaluated by A/B tests and user feedback rather than a published gold set ([LinkedIn Engineering, Dec 2023](https://www.linkedin.com/blog/engineering/skills-graph/extracting-skills-from-content)). Distilling LLM labels into encoders is established. UniversalNER beats ChatGPT by 7 to 9 F1 ([arXiv 2308.03279](https://arxiv.org/abs/2308.03279)). NuNER's gains are log-linear in data, from 60.3 F1 at 1k examples to 64.8 at 1M ([arXiv 2402.15343](https://arxiv.org/html/2402.15343)).

Evaluation without a gold set is where the evidence turns against us. Across 350+ LLMs, when two models both err they choose the same wrong answer 60% of the time, and same-provider models are the most correlated ([Kim et al., ICML 2025](https://arxiv.org/pdf/2506.07962)). LLM judges prefer their own outputs ([Panickssery et al., arXiv 2404.13076](https://arxiv.org/abs/2404.13076)), and judge reliability varies enough across tasks that the JUDGE-BENCH authors say LLM judges must be validated against humans ([ACL 2025](https://arxiv.org/abs/2406.18403)). In attribute extraction, the choice of exact vs. fuzzy matching moved F1 about 23 times more than the choice of model ([Beyond Exact Match, arXiv 2607.22949](https://arxiv.org/abs/2607.22949)). The defensible method is a small human sample combined with large-scale model judgments through prediction-powered inference, which gives valid confidence intervals ([Angelopoulos et al., Science 2023](https://arxiv.org/abs/2301.09633)). Stratified PPI tightens the intervals most when autorater accuracy varies across strata ([Fisch et al., NeurIPS 2024](https://arxiv.org/abs/2406.04291)). Confidence-driven selection cut the human labels needed by over 25% ([Gligoric et al., arXiv 2408.15204](https://arxiv.org/abs/2408.15204)). Prompt optimizers such as MIPRO report gains of up to 13%, but they need that labeled dev set ([arXiv 2406.11695](https://arxiv.org/abs/2406.11695)).

### How our design compares

Haiku recovered 82% of Opus's taught concepts and agreed on teaching status 88% of the time, at about a 21x lower cost ([design doc §9](courseware_knowledge_extraction.md)). That is inside the range the cascade papers report. Those papers measure against gold labels, though, and we measure one Claude model against another. Shared misses are invisible to this proxy, so recall is the number most likely to be inflated. The design doc already resolves both models' labels together before comparing. That is the right matching rule, and it needs to be fixed and reported with every agreement figure, given how much matching protocol moves F1. The tiering split (Haiku for reading and judgment, Aho-Corasick for propagation, GLiNER2 only for discovery) matches LinkedIn's architecture. Our GLiNER2 run (0.91 recall of text-present labels at 1.6k chars/s) suggests distillation should target what span models do not do zero-shot: paraphrased mentions and teaching-status classification.

### Recommended changes

| Rank | Change | Impact | Effort |
|---|---|---|---|
| 1 | Build a human sample. For precision, label 300 to 400 emitted claims (about ±3.5 to 5 pp at p≈0.85, my calculation), sampled by course first. For recall, exhaustively annotate 20 to 40 subsections, part of them blind to model output | High | Medium |
| 2 | Freeze the matching rule (normalized label, alias, then resolved concept) and report it with every agreement number | High | Low |
| 3 | Estimate per-stratum precision with stratified PPI, stratified by course family and Haiku/Opus agreement status, with Opus as autorater; oversample disagreement strata | High | Medium |
| 4 | Escalate to Opus by disagreement or low self-consistency, not only by random sample and "decision-critical" claims | Medium | Medium |
| 5 | Use a non-Claude judge or second extractor for some checks to break family correlation | Medium | Low |
| 6 | After the gold set exists: DSPy/MIPRO on the Haiku prompt; later, a distilled teaching-status classifier trained on Opus-filtered labels | Medium | High |

## 6. Append-only versioning is ahead of practice; carry-forward and promotion are not designed

### What practice and research say

Every published production KG runs the same curation loop. A model proposes, a human accepts, and accepted decisions become training, critic or prompt data (LinkedIn taxonomists; COSMO's reviewers distill principles back into prompts; Coursera's instructor and learner tags) ([LinkedIn, Mar 2023](https://www.linkedin.com/blog/engineering/data/building-maintaining-the-skills-taxonomy-that-powers-linkedins-skills-graph); [Amazon Science COSMO blog](https://www.amazon.science/blog/building-commonsense-knowledge-graphs-to-aid-product-recommendation); [Coursera methodology](https://www.coursera.org/skills-reports/campus/methodology)). None documents how it versions prompts or models or handles re-extraction. Re-runs are not reproducible even at temperature 0. One 2026 study got 83 and 86 entities on identical inputs and froze extraction output to keep the downstream steps deterministic. It ran only two runs per condition ([arXiv 2609.11493](https://arxiv.org/pdf/2609.11493), search snippet only). Langfuse treats each prompt save as an immutable version and uses movable `production`/`staging` labels for promotion and rollback ([Langfuse docs](https://langfuse.com/docs/prompts)).

Lakehouse as system of record with the graph as a projection is established at taxonomy scale. LinkedIn's Skills Graph (39k skills, 200k+ edges) is built by a batch pipeline and served through a Rest.li service plus HDFS, not a graph DB ([LinkedIn](https://www.linkedin.com/blog/engineering/data/building-maintaining-the-skills-taxonomy-that-powers-linkedins-skills-graph)). Databricks' reference GraphRAG pattern keeps data in Delta and projects it into Neo4j ([Databricks, Apr 2025](https://databricks.com/blog/building-improving-and-deploying-knowledge-graph-rag-systems-databricks)). Netflix's UDA binds an RDF domain model to existing Iceberg tables, though I only read secondary sources on it ([TLDR Data, June 2025](https://tldr.tech/data/2025-06-16)). Teams that went graph-DB-first did so for online, deep traversals at billions of edges, and they still snapshot to the warehouse ([Airbnb identity graph](https://airbnb.tech/?p=804)).

### How our design compares

Keying rows by a content hash of extractor, prompt, schema, model, effort and input has no direct counterpart in the papers I reviewed. It is stricter than common practice, and it is the right response to non-determinism and forced model retirement. Three gaps remain. First, `claim_pk` hashes `extraction_run_fk`, so every re-extraction mints new claim keys. Acceptance has to carry forward by stable concept ID and claim content rather than by row, and no source documents how to do this. Second, `is_current` picks the newest row per scope. A new prompt version therefore goes live as soon as it runs, before anyone has compared it, whereas Langfuse requires a deliberate label move for the same promotion. Third, review load grows with release cadence times corpus size, and I found no published figures on reviewer throughput. Drift monitoring for extraction is not described in any primary source. The signals below are my proposals, not documented practice.

### Recommended changes

| Rank | Change | Impact | Effort |
|---|---|---|---|
| 1 | Replace "newest wins" with an explicit promoted `extraction_run` (or extractor version) pointer per scope family, moved only after a diff against the previous version | High | Low |
| 2 | Carry acceptance forward: a new run's claim that reproduces an accepted (concept ID, kind, status, scope) inherits acceptance; only new or changed claims enter review | High | Medium |
| 3 | Monitor per-version label and status distributions, old-vs-new agreement on the same input hash, and acceptance rate per version as dbt tests or Superset charts | Medium | Low |
| 4 | Treat a model retirement as a scheduled bulk re-extraction plus diff and sampled re-review, budgeted against the 3 to 6 month notice window | Medium | Low |

## 7. At low millions of edges, tables plus a Parquet export beat a resident graph database

### What practice and research say

Dedicated graph stores earned their place at LinkedIn (LIquid, about 270B edges, about 2M QPS) and Airbnb (7B nodes, 11B edges, 4 to 8 hop traversals, read P95 down from 2.1 s to 1.0 s) ([LinkedIn Engineering 2023](https://engineering.linkedin.com/blog/2023/how-liquid-connects-everything-so-our-members-can-do-anything); [Airbnb](https://airbnb.tech/?p=804)). Relational engines with purpose-built path operators hold up well. DuckPGQ's CSR plus multi-source BFS beat Neo4j Enterprise 4.4 on path-finding and pattern matching up to 670M edges, while naive recursive SQL in Umbra hit OOM. The authors call recursive formulations "slow and brittle" ([DuckPGQ, CIDR 2023](https://www.cidrdb.org/cidr2023/papers/p66-wolde.pdf)). Apache GraphAr defines a Parquet labeled-property-graph layout for data lakes ([PVLDB vol. 18](https://www.vldb.org/pvldb/vol18/p530-li.pdf)). Engine continuity is a real risk. Kuzu was archived in October 2025 and continues only as the LadybugDB fork ([FreshPorts](https://www.freshports.org/databases/kuzu/); [DBDB](https://dbdb.io/db/ladybugdb)). Wikidata's dependence on unmaintained Blazegraph forced a graph split in May 2025 that broke existing queries ([WikiCite graph split](https://meta.wikimedia.org/wiki/WikiCite/WDQS_graph_split)).

For retrieval, GraphRAG helps on multi-hop, temporal and comparison questions and hurts on single-fact lookup. Retrieving KG triples alone found the answer 39% of the time on HotpotQA, against 89% for plain RAG, and hybrid vector-plus-graph retrieval scored best at 70B ([Han et al., arXiv 2502.11371](https://arxiv.org/html/2502.11371v2)). GraphRAG-Bench measured MS-GraphRAG global at 331,375 prompt tokens per query against 879 for RAG, and found it 11.6 to 26.1 points worse on simple fact retrieval. HippoRAG2 stayed near RAG's token cost ([GraphRAG-Bench, ICLR 2026](https://arxiv.org/html/2506.05690v3)). Education evidence is small. A KG-RAG tutor beat a RAG tutor with 76 students (d=0.86) ([arXiv 2311.17696](https://arxiv.org/abs/2311.17696v7)). Khan Academy's 20 A/B tests over 15M+ tutoring threads found that injecting "unmastered prerequisite skills with a brief review" raised next-item correctness by 2.7%. Plain-text context beat JSON, and no graph retrieval was involved ([Khan Academy, May 2026](https://blog.khanacademy.org/how-khan-academy-is-building-a-better-ai-tutor-our-most-recent-learnings/)). Duolingo stores learner model state as one DynamoDB row per learner per course ([Duolingo blog](https://blog.duolingo.com/unique-engineering-problems)).

### How our design compares

Our graph without learners is low millions of edges, consumed in batch, with prerequisite closure precomputed. That is three to four orders of magnitude below the thresholds that drove graph-DB adoption, so tables as system of record with a per-label Parquet export is the established pattern for our case. The export is engine-neutral, which hedges continuity risk for DuckPGQ (single maintainer) and LadybugDB (young fork). Serving matches the evidence too: a deterministic context document for hybrid search, structured prerequisites for the tutor by key lookup, and evidence passages joined by key instead of bare triples.

### Recommended changes

| Rank | Change | Impact | Effort |
|---|---|---|---|
| 1 | Keep the projection; do not adopt a resident graph DB until a consumer needs low-latency k-hop expansion with ranking | High | None |
| 2 | Lay out the Parquet export to follow GraphAr's schema metadata so any loader can read it | Low | Low |
| 3 | For learn-ai, inject unmastered prerequisites and recent attempts as plain text by lookup; replicate Khan's plain-text-vs-JSON comparison | Medium | Low |
| 4 | Skip community-summary GraphRAG. If graph retrieval is tested, use HippoRAG2-style personalized PageRank computed from exported edges | Medium | Medium |
| 5 | Keep learner edges in warehouse facts plus a row-per-learner cache under a separate access policy, never in the published export | Medium | Low |

## Risks we have not considered

The course header is inside the extraction hash. `extraction_key` hashes the full message, and every scope's message includes the warehouse-built title, outline and description ([design doc §9](courseware_knowledge_extraction.md)). An edit to the outline or description therefore re-extracts every subsection in the course, mints new `claim_pk`s and pushes the whole course back into review. Haiku keeps the dollar cost small. The review churn is the problem, and it compounds the carry-forward gap in Q6. Hashing the scope text and a header digest separately, and re-extracting on header change only when the outline changed materially, would contain it. This is my inference from the design, not an observed failure.

Propagation multiplies resolution errors. Aho-Corasick tags every block in the corpus with each alias. A wrong merge, or a short generic alias ("R", "recall", "kernel", which the design doc already flags), becomes thousands of false mentions instead of one bad row. Deg-RAG saw related entities absorbed at high reduction rates ([arXiv 2510.14271](https://arxiv.org/html/2510.14271v1)). Propagation should read only from accepted concepts and aliases.

The review gate decides what scales. Only accepted concepts reach Learn, and LinkedIn needs a taxonomist team for about 39k skills ([LinkedIn](https://www.linkedin.com/blog/engineering/data/building-maintaining-the-skills-taxonomy-that-powers-linkedins-skills-graph)). At 50k to 500k labels, either Learn sees little or the gate gets bypassed. The way through is calibrated auto-acceptance for high-confidence exact matches, as in OpenRefine's score-faceted bulk accept ([OpenRefine](https://openrefine.org/docs/manual/reconciling)), with humans reviewing only the uncertain band and a sample.

Wikidata QIDs are not permanent identities. Duplicates get merged into redirects, and links are repointed later ([Wikidata Help:Redirects](https://www.wikidata.org/wiki/Help:Redirects)), so a local dump index has to follow redirects at each refresh. Silent provider-side changes also escape the content hash. Bedrock exposes no seed ([arXiv 2609.11493](https://arxiv.org/pdf/2609.11493), snippet), and a pinned model ID does not guarantee unchanged serving behavior ([arXiv 2307.09009](https://arxiv.org/abs/2307.09009v1)). Periodically re-running a fixed canary set of scopes and diffing against stored output would detect this.

Prompt examples can leak into labels. LangExtract warns that few-shot example text can be extracted instead of source text ([LangExtract README](https://github.com/google/langextract)). Quote verification catches leaked quotes but not leaked concept labels, which carry no quote requirement of their own. Learner-facing descriptions are ungrounded generation. Haiku writes them from claim descriptions without verifying them against evidence, so they need the same review and support check as claims before they reach chips and tooltips. Measurement protocol can swamp model differences: without a fixed matching rule, a prompt change can look like a regression or an improvement for reasons unrelated to quality ([arXiv 2607.22949](https://arxiv.org/abs/2607.22949)).

## Conclusion

The literature changes where the engineering effort should go. Extraction quality, which the proof of concept spent most of its effort on, is the most settled part of the field and the part where we already match or exceed product systems. The open problems are identity, support and measurement. The 2023-2026 entity-matching work converges on small candidate sets with relation-typed verdicts. MDM and Wikidata practice settles identity with opaque IDs and redirects. The statistics literature (PPI and its stratified variants) makes a few hundred human labels enough to put honest intervals on precision. None of these needs new infrastructure, and all of them fit dbt tables and Dagster assets. The cheapest high-value move is the identity change, because every later review decision depends on keys that survive reruns.

The evidence is thinnest exactly where our design is most original: an explicit taught/used status, course-level `requires` edges, and claims typed by Bloom process. No benchmark exists for any of these, so we will have to build the first evaluation ourselves. Our data offers a channel most published work lacks, which is problem attempts mapped to concepts. Most prerequisite graphs in the literature were never checked against learner outcomes, and Khan Academy, the one large deployment with a prerequisite map, removed it. A modest learner-outcome check on a sample of inferred edges would therefore be both a quality gate and a result worth publishing.

## References

KG construction pipelines and grounding

- Edge et al., "From Local to Global: A Graph RAG Approach", arXiv 2404.16130, Apr 2024, rev. 19 Feb 2025. https://arxiv.org/html/2404.16130v2
- Microsoft GraphRAG `defaults.py` (main branch, read 2026-10-16). https://raw.githubusercontent.com/microsoft/graphrag/main/packages/graphrag/graphrag/config/defaults.py
- Microsoft GraphRAG configuration docs (read 2026-10-16). https://microsoft.github.io/graphrag/config/yaml/
- Microsoft GraphRAG default dataflow (read 2026-10-16). https://microsoft.github.io/graphrag/index/default_dataflow/
- Microsoft Research, "LazyGraphRAG", 25 Nov 2024. https://www.microsoft.com/en-us/research/blog/lazygraphrag-setting-a-new-standard-for-quality-and-cost/
- Guo et al., "LightRAG", arXiv 2410.05779, Oct 2024, rev. 28 Apr 2025. https://arxiv.org/html/2410.05779v3
- Neo4j, neo4j-graphrag-python KG builder guide (read 2026-10-16). https://neo4j.com/docs/neo4j-graphrag-python/current/user_guide_kg_builder.html
- Mo et al., "KGGen", arXiv 2502.09956, Feb 2025, rev. 6 Nov 2025 (NeurIPS 2025). https://arxiv.org/html/2502.09956v2
- Zhang and Soh, "Extract, Define, Canonicalize", arXiv 2404.03868, Oct 2024 (EMNLP 2024). https://arxiv.org/html/2404.03868
- Caufield et al., "SPIRES / OntoGPT", Bioinformatics 40(3), 21 Feb 2024. https://academic.oup.com/bioinformatics/article/40/3/btae104/7612230
- Google LangExtract README (v1.6, 2026) and `resolver.py` (read 2026-10-16). https://github.com/google/langextract ; https://raw.githubusercontent.com/google/langextract/main/langextract/resolver.py
- docling-graph README (read 2026-10-16). https://github.com/docling-project/docling-graph
- Khorshidi et al., "ODKE+", arXiv 2509.04696, Sept 2025. https://arxiv.org/abs/2509.04696
- Bai et al., "AutoSchemaKG", arXiv 2505.23628, May 2025. https://arxiv.org/abs/2505.23628
- Bian, survey of LLM-based KG construction, arXiv 2510.20345, 23 Oct 2025. https://arxiv.org/abs/2510.20345
- Diffbot, confidence score docs (read 2026-10-16). https://docs.diffbot.com/docs/confidence-score
- Amazon Science, "COSMO", SIGMOD 2024. https://www.amazon.science/publications/cosmo-a-large-scale-e-commerce-common-sense-knowledge-generation-and-serving-system-at-amazon ; blog: https://www.amazon.science/blog/building-commonsense-knowledge-graphs-to-aid-product-recommendation
- Anthropic, "Contextual Retrieval", 19 Sept 2024. https://www.anthropic.com/engineering/contextual-retrieval
- Anthropic, Claude citations docs (read 2026-10-16). https://platform.claude.com/docs/en/build-with-claude/citations
- Gao et al., "ALCE", arXiv 2305.14627, EMNLP 2023. https://arxiv.org/abs/2305.14627
- Li et al., "AttributionBench", arXiv 2402.15089, Findings of ACL 2024. https://arxiv.org/pdf/2402.15089
- Wallat et al., "Correctness is not faithfulness", arXiv 2412.18004, Dec 2024. https://arxiv.org/pdf/2412.18004
- Kumar, "ExtractConf", arXiv 2606.24420, June 2026. https://arxiv.org/abs/2606.24420
- Zaratiana et al., "GLiNER", arXiv 2311.08526, NAACL 2024. https://arxiv.org/abs/2311.08526

Entity resolution and identity

- Peeters, Steiner and Bizer, "Entity Matching using LLMs", arXiv 2310.11244, v4 18 Oct 2024 (EDBT 2025). https://arxiv.org/abs/2310.11244v4
- Steiner, Peeters and Bizer, "Fine-tuning LLMs for Entity Matching", arXiv 2409.08185, v2 21 May 2025. https://arxiv.org/html/2409.08185v2
- Wang et al., "Match, Compare, or Select?", arXiv 2405.16884, 12 Dec 2024 (COLING 2025). https://arxiv.org/html/2405.16884v3
- Fu et al., "LLM-CER", arXiv 2506.02509, June 2025 (SIGMOD 2026). https://arxiv.org/html/2506.02509v1
- "Less is More: Denoising Knowledge Graphs for RAG" (Deg-RAG), arXiv 2510.14271, 16 Oct 2025. https://arxiv.org/html/2510.14271v1
- GenOM, arXiv 2508.10703, 2025. https://arxiv.org/pdf/2508.10703
- Christen et al., graph-metrics-driven cluster repair, ACM JDIQ 17, June 2025. https://dbs.uni-leipzig.de/files/research/publications/2025-6/pdf/graph_metrics_driven_cluster_repair.pdf
- Pires and Junior, "Strong Matchers, Weak Architecture", SBBD 2026. https://sol.sbc.org.br/index.php/sbbd/article/view/44035
- Splink cluster evaluation docs (read 2026-10-16). https://moj-analytical-services.github.io/splink/topic_guides/evaluation/clusters/overview.html
- LinkedIn Engineering, "Building and maintaining the skills taxonomy", 21 Mar 2023. https://www.linkedin.com/blog/engineering/data/building-maintaining-the-skills-taxonomy-that-powers-linkedins-skills-graph
- OpenRefine reconciliation manual (read 2026-10-16). https://openrefine.org/docs/manual/reconciling
- Tamr, publishing clusters docs (read 2026-10-16). https://docs.tamr.com/new/docs/publishing-clusters
- Reltio, merging and survivorship docs (read 2026-10-16). https://docs.reltio.com/matchmerge/merginginreltio.html ; https://docs.reltio.com/en/model/consolidate-data/design-survivorship-rules
- Wikidata, Help:Redirects (proposed policy, read 2026-10-16). https://www.wikidata.org/wiki/Help:Redirects
- van Staden, semantic ID stability, arXiv 2409.18254, 26 Sept 2024. https://arxiv.org/abs/2409.18254
- van Staden and Grubb, "ABCDE", arXiv 2407.21430, 31 July 2024. https://www.arxiv.org/abs/2407.21430
- Binette et al., ER evaluation estimators, arXiv 2404.05622, 2024. https://arxiv.org/pdf/2404.05622

Educational KGs and prerequisites

- Pan et al., "Prerequisite Relation Learning for Concepts in MOOCs", ACL 2017. https://aclanthology.org/P17-1133/
- "Prerequisite Relation Learning: A Survey and Outlook", ACM Computing Surveys, 2025. https://dl.acm.org/doi/10.1145/3733593
- GraphRAG learning path recommendation, arXiv 2506.22303, 2025. https://arxiv.org/html/2506.22303v1
- Liang et al., "AL-CPL", EAAI 2018. https://clgiles.ist.psu.edu/pubs/eaai18.pdf
- PRELEARN @ EVALITA 2020, arXiv 2011.03760. https://arxiv.org/pdf/2011.03760
- ProPRL, arXiv 2608.03006, 4 Aug 2026. https://arxiv.org/pdf/2608.03006
- Yang et al., LLM prerequisite prediction, arXiv 2402.14293, Feb 2024. https://arxiv.org/html/2402.14293
- Graphusion, arXiv 2407.10794, July 2024. https://arxiv.org/html/2407.10794v1
- Bloom classification of learning objectives, EDM 2022. https://educationaldatamining.org/edm2022/proceedings/2022.EDM-short-papers.55/index.html
- Bloom classification of outcomes and exam questions, arXiv 2511.10903, Nov 2025. https://www.arxiv.org/pdf/2511.10903
- Chen et al., prerequisite discovery from performance data, EDM 2015. https://www.educationaldatamining.org/EDM2015/uploads/papers/paper_42.pdf
- COMMAND, EDM 2016. https://www.educationaldatamining.org/EDM2016/proceedings/paper_89.pdf
- Coursera Skills Report methodology (undated, read 2026-10-16). https://www.coursera.org/skills-reports/campus/methodology
- Khan Academy support thread on the removed knowledge map (read 2026-10-16). https://support.khanacademy.org/hc/en-us/community/posts/360027982751-What-happened-to-the-knowledge-map

Model tiering and evaluation

- Chen, Zaharia and Zou, "FrugalGPT", arXiv 2305.05176, 9 May 2023. https://arxiv.org/abs/2305.05176
- Ong et al., "RouteLLM", arXiv 2406.18665, June 2024, v4 Feb 2025. https://arxiv.org/abs/2406.18665
- Nie et al., "Online Cascade Learning", ICML 2024, arXiv 2402.04513. https://arxiv.org/abs/2402.04513
- LinkedIn Engineering, "Extracting skills from content", 13 Dec 2023. https://www.linkedin.com/blog/engineering/skills-graph/extracting-skills-from-content
- Zhou et al., "UniversalNER", arXiv 2308.03279, ICLR 2024. https://arxiv.org/abs/2308.03279
- Bogdanov et al., "NuNER", arXiv 2402.15343, 23 Feb 2024. https://arxiv.org/html/2402.15343
- Kim et al., "Correlated errors in LLMs", ICML 2025, arXiv 2506.07962. https://arxiv.org/pdf/2506.07962
- Panickssery, Bowman and Feng, self-preference, arXiv 2404.13076, 15 Apr 2024. https://arxiv.org/abs/2404.13076
- Bavaresco et al., "JUDGE-BENCH", ACL 2025, arXiv 2406.18403. https://arxiv.org/abs/2406.18403
- Dwivedi and Soni, "Beyond Exact Match", arXiv 2607.22949, 24 July 2026. https://arxiv.org/abs/2607.22949
- Angelopoulos et al., "Prediction-powered inference", Science 2023, arXiv 2301.09633. https://arxiv.org/abs/2301.09633
- Fisch et al., "Stratified PPI", NeurIPS 2024, arXiv 2406.04291. https://arxiv.org/abs/2406.04291
- Gligoric et al., "Confidence-Driven Inference", arXiv 2408.15204, rev. 8 Feb 2025. https://arxiv.org/abs/2408.15204
- Opsahl-Ong et al., "MIPRO", arXiv 2406.11695, rev. 6 Oct 2024. https://arxiv.org/abs/2406.11695

Operations, storage and serving

- Chen, Zaharia and Zou, "How is ChatGPT's behavior changing over time?", arXiv 2307.09009, July 2023. https://arxiv.org/abs/2307.09009v1
- Non-reproducible entity extraction study, arXiv 2609.11493, Sept 2026 (search snippet only). https://arxiv.org/pdf/2609.11493
- OpenAI deprecations (read 2026-10-16). https://developers.openai.com/docs/deprecations
- Azure OpenAI model retirements (read 2026-10-16). https://learn.microsoft.com:443/en-us/azure/ai-services/openai/concepts/model-retirements
- Langfuse prompt management docs (read 2026-10-16). https://langfuse.com/docs/prompts
- Airbnb Tech Blog, "Scaling Airbnb's identity graph" (undated, c. May 2026). https://airbnb.tech/?p=804
- LinkedIn Engineering, "How LIquid connects everything", 2023. https://engineering.linkedin.com/blog/2023/how-liquid-connects-everything-so-our-members-can-do-anything
- Databricks, "Building, improving and deploying knowledge graph RAG systems", 1 Apr 2025. https://databricks.com/blog/building-improving-and-deploying-knowledge-graph-rag-systems-databricks
- TLDR Data on Netflix UDA (secondary), 16 June 2025. https://tldr.tech/data/2025-06-16
- Li et al., "Apache GraphAr", PVLDB vol. 18. https://www.vldb.org/pvldb/vol18/p530-li.pdf
- Wolde et al., "DuckPGQ", CIDR 2023. https://www.cidrdb.org/cidr2023/papers/p66-wolde.pdf
- FreshPorts, kuzu (archival record, read 2026-10-16). https://www.freshports.org/databases/kuzu/ ; DBDB, LadybugDB: https://dbdb.io/db/ladybugdb
- WikiCite, WDQS graph split (May 2025). https://meta.wikimedia.org/wiki/WikiCite/WDQS_graph_split
- Han et al., "RAG vs. GraphRAG", arXiv 2502.11371, v2 17 Oct 2025. https://arxiv.org/html/2502.11371v2
- Xiang et al., "GraphRAG-Bench", ICLR 2026, arXiv 2506.05690, v3 22 Feb 2026. https://arxiv.org/html/2506.05690v3
- KG-RAG tutor study, arXiv 2311.17696, rev. Feb 2025. https://arxiv.org/abs/2311.17696v7
- Khan Academy, "How Khan Academy is building a better AI tutor", 6 May 2026. https://blog.khanacademy.org/how-khan-academy-is-building-a-better-ai-tutor-our-most-recent-learnings/
- Duolingo, "Unique engineering problems" (undated). https://blog.duolingo.com/unique-engineering-problems

Internal

- Courseware knowledge extraction design, sections 4 to 9 (2026-10-16). `docs/design/courseware_knowledge_extraction.md`
- Concept key derivation. `src/ol_dbt/models/intermediate/courseware/int__courseware__concept_keys.sql`, `src/ol_dbt/models/dimensional/dim_concept.sql`
- Prerequisite rule. `src/ol_dbt/models/dimensional/bridge_courserun_concept.sql`
