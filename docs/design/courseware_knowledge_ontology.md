# Courseware knowledge ontology: terms, content levels, roles and what extraction records

Status: draft for team agreement (2026-11-18). This is the foundation the rest of the courseware knowledge work builds on: [the design document](courseware_knowledge_extraction.md), the [annotation guideline](courseware_annotation_guideline.md), the Learn ingest views and the `course_knowledge` tenant. Where this document and the design document disagree on a term, this one is the proposal and the design document changes once it is agreed.

It combines three inputs: the team's vocabulary document (a shared Google Doc with levels, roles, a per-platform mapping and Ziq's schema.org/SKOS/Web Annotation mapping), the draft and the MIT Learn measurement recorded on the witan task "Agree a shared vocabulary for content granularity and composition across platforms and modalities", and a Wikidata probe run for this document. Sections 3 to 8 are written as the target state so they read as one ontology; none of it is agreed until the team settles the decisions in section 9. Each section and decision has a heading that names it, so it can be cited from other repos.

Two words used throughout: a facet is one model call over a lesson's assembled text with its own prompt and schema version (today there is one, the text stage of `courseware_claims_v2`); UAI refers to the five MITx Online UAI courses (UAI.1 to UAI.5, 1T2026) that are the only production extractions.

## 1. Why this has to be settled before the backfill

The text stage of extraction is keyed by the extractor, prompt version, schema version, model, effort and the sha256 of the exact message (`text_extraction_key` in `ml.lib.courseware.extract`). Any change to what the text stage is asked to record therefore re-reads every lesson of every run. Today production holds extractions for five UAI runs only (`seeds/courseware_extraction_allowlist.csv`). The local corpus measured in October had 6,224 Open edX runs and 3,260 distinct source texts. Whatever the text stage does not record before that backfill costs a second backfill later.

The other decisions here are cheaper to change. Levels, roles and the platform crosswalk are dbt models over stored rows, and renaming a relation is a view change. They still need to be agreed first, because Learn (`Unit`, `UnitContentFile`), the tenant routes and the gold-set guideline all speak at some granularity, and a term that means two levels in two places makes claims from different platforms incomparable.

So the order is: agree the terms and levels (sections 3 to 5), agree what the text stage records (section 6), then version the schema once and backfill.

## 2. Use cases the ontology has to serve

From the team vocabulary document and the design document. Each line says what it needs from the ontology, which is how the term list in section 3 was checked for gaps.

- Search relevance and product pages in MIT Learn and AskTIM: what a course teaches, at the lesson where it teaches it, with labels in more than one language, and what it says is required or optional.
- Customized learning pathways: what a course assumes, at what depth it treats each concept, and course-to-course preparation kept separate from concept-to-concept hierarchy.
- Customized B2B offerings: the same, plus the industries, organizations and case studies a course works with, and which lessons can be recombined.
- Digital credential skill attestations: skills tied to the assessments that exercise them, and skills tied to the knowledge they act on.
- Sparsity and density of taught concepts and skills across the catalog: one cross-course identity per concept and a role for every piece of content, so a reference reading does not count as teaching.
- Instructor and people discovery ("courses taught by X", "who presents this lesson"): people as entities with a role in the material, linked to the catalog's instructors.
- Content maintenance: statements that date a course (software versions, "as of 2021"), so stale material can be found without re-reading it.
- Tutoring and assessment design (AskTIM's tutor, course teams): the misconceptions the material itself warns about, tied to the concepts they concern.
- Reading lists and research links: the books, papers and software a course cites, with identifiers that link to OpenAlex works and authors.

## 3. Term definitions

Definitions follow W3C SKOS where it has a term, as the vocabulary document suggests. The published mapping column is what JSON-LD and schema.org markup emit; `mitol:` is our extension namespace (the vocabulary document calls it `mitlocal:`; one prefix needs choosing, see decision 9 in section 9).

### Vocabulary entries have two types: knowledge and skill

A vocabulary entry is one resolved, cross-course term with an opaque UUID (design document section 10). It has a `term_type`:

- knowledge: an abstract representation of knowledge that is useful in understanding a subject or addressing a problem, e.g. "eigenvector". Published as `skos:Concept` and `schema:DefinedTerm` with `mitol:termType knowledge`.
- skill (competency): a demonstrated ability with a concept, tool or technique, used to address a problem, e.g. "compute the eigenvectors of a matrix". Phrased as an action. Published the same way with `mitol:termType skill`.

Knowledge entries carry a `knowledge_kind`: `concept` (an idea or relation), `method` (an algorithm or procedure), `tool` (software, a language, an instrument), `dataset`, `application` (a problem domain a method is applied to).

This settles a conflict in what exists today. The extraction schema (`ConceptClaim.concept_kind`) has `concept, method, tool, dataset, application` and keeps skills as a separate claim kind. The design document's `dim_concept.concept_kind` and Learn's `ConceptOutput.kind` list `skill` as a kind of concept. Under this proposal skill is a term type, not a kind, so a skills report and a concept graph can each filter on one column, and `dataset` stays a kind.

A skill names the knowledge it acts on (`mitol:exercises`, skill to knowledge, many to many) and a Bloom process (`apply`, `analyze`, ...). Without that link an attestation can say a learner passed an assessment of "fit a k-means model in R" but not that it involves k-means or R.

### A sense is what a label means in one course run

A label can name different things in different fields ("kernel" in operating systems, linear algebra and SVMs). A sense is the assignment of a label, within one course run, to one vocabulary entry (design document section 10, one sense per discourse). Claims join to entries through senses, never through labels.

### Prerequisite is four different assertions

"Prerequisite" is used for four things that must stay separate columns and predicates, because they carry different evidence and different authority:

| assertion | defined as | source | published as |
|---|---|---|---|
| declared prerequisite | the material states that a learner needs, or is recommended to have, this knowledge or course | text stage, quoted | `schema:coursePrerequisites` (course), `schema:competencyRequired` (knowledge) |
| assumed knowledge | the material relies on it without explaining it | text stage, quoted | `mitol:assumes` |
| preparation candidate | derived: a course relies on a concept before teaching it, in enough lessons and courses to pass the support rule | dbt rule | not published until evaluated |
| concept dependency | engaging effectively with one concept requires another, at a stated depth and audience (the vocabulary document's definition) | review only | `mitol:requiresConcept` |

The vocabulary document's definition ("the concepts necessary to engage effectively with the concept or skill being presented") is the fourth row. Nothing automated produces it, which is what the publication-contract section of the design document already requires.

### Teaching status and treatment depth are separate properties of a claim

`teaching_status` says whether a lesson teaches a term: `taught` (defined, explained, derived or demonstrated), `reviewed` (explained again as something learners have seen), `used` (applied without explanation), `mentioned` (named only).

`treatment` is new and says how deeply a lesson that teaches or reviews a term treats it. It is a set, since a lesson can do more than one:

- `intuitive`: explained informally, by analogy, picture or example
- `formal`: defined precisely, derived or proved
- `computational`: computed by hand or implemented in code

Depth is what the pathways and preparation use cases need ("this course treats eigenvectors intuitively; that one requires them formally"), and the design document's phasing already says preparation has to be evaluated at a competency depth. It can only come from reading the text, so it belongs in the text stage.

### Observations, evidence and spans

- Claim (observation): one assertion the text stage makes about one lesson, with its evidence. Append-only, never edited, keyed by its extraction. Published as an `rdf:Statement` with `rdf:subject` (the lesson or item), `rdf:predicate` and `rdf:object` (the term), plus `mitol:evidence`.
- Evidence: a verbatim passage of 8 to 60 words from one item, verified against the item's text. Published as `oa:SpecificResource` with `oa:hasSource` (the item) and an `oa:TextQuoteSelector`.
- Span: a position inside an item used as an evidence locator: a character range (`oa:TextPositionSelector`), a transcript or video time range (`oa:FragmentSelector` with Media Fragments), a page range, a slide, or one part of a multi-part problem. A span is not a level (section 4).

### Other claim kinds keep their current meaning

Learning outcome (a statement about what a learner will be able to do, distinct from a term, with Bloom process and knowledge type), assessment expectation (what a graded item asks, from the question side only), and audience (who the material says the course is for) keep the definitions in the design document's model section.

## 4. Content levels, roles and the platform crosswalk

This adopts the team vocabulary document (decision 1 in section 9). The draft on the shared-vocabulary task matches it, and Learn's mapping comes from the 2026-11-10 measurement on the same task.

### Levels are defined by role, ordered by containment, and may be implicit

Largest to smallest:

| level | definition | published as |
|---|---|---|
| course | the identity of an offering across time; has one or more course runs | `schema:Course` |
| course run | one delivery of a course, with its own content snapshot, dates and learners | `schema:CourseInstance` (`schema:hasCourseInstance`) |
| module | an optional, author-named group of lessons presented as a major division | `schema:LearningResource`, `mitol:structuralLevel module` |
| lesson | the smallest named, ordered stop that carries its own learning focus; content inside a lesson is read together to decide whether a term is taught, reviewed or used there; the extraction scope | same, `lesson` |
| segment | an optional, ordered, titled subdivision of a lesson that groups items | same, `segment` |
| item | a single learning object with its own identity, kind and content; the smallest addressable level | the kind's type (below), `item` |

Containment is `schema:hasPart` / `schema:isPartOf`; order is `schema:ItemList` with `schema:position`.

Rules: any level except item may be absent in a source, and an absent one is created as implicit (`is_implicit = true`) so every item has a lesson and every lesson a course run. Items no lesson places (OCW documents no page links, today's `materials` scope, Learn's bin files) go into one implicit lesson per run with role `reference`. Every level assignment records its basis: `native` (a platform type), `rule` (our heuristic) or `override` (a person decided).

### Span, asset, item kind and composition rule are not levels

- Span: a position inside an item (section 3). A video chapter is a span, not a segment.
- Asset: a file an item reads or links (mp4, caption file, PDF, slide deck, `/static/` file). `schema:MediaObject` with `schema:contentUrl` and `schema:encodingFormat`; `mitol:usesAsset` from the item.
- Item kind: `page` (`schema:WebPage`), `video` (`schema:VideoObject`), `document` (`schema:DigitalDocument`), `problem` (`schema:Question`, or `schema:Quiz` for a set), `discussion` (`mitol:Discussion`: an Open edX discussion block is a forum topic, not a post, so `schema:DiscussionForumPosting` does not fit), `link`.
- Composition rule: changes what a learner sees without adding a level: randomized pools, conditional release, A/B branches, completion requirements. `mitol:compositionRule`.

### Five roles, separate from level and pacing

Role is `schema:educationalUse`, a closed set:

- `context`: describes the course or a module and does not teach (about pages, syllabus, overview, welcome, wrap-up)
- `instruction`: presents the material (lecture, reading, demonstration)
- `practice`: ungraded or low-stakes work (recitations, worked examples, exercises)
- `assessment`: graded work (assignments, problem sets, quizzes, exams)
- `reference`: material the course points to without teaching from it (textbook sections, unlinked documents)

A pacing label (week 3, session 12, lecture 4, a due date) is a schedule property, not a level and not a role.

Context keeps the module it describes. Learn collapses overview and summary sequentials into one `Unit(kind=context)` per run, which loses that link. Our extraction sources do the same today (every context sequential is unioned into one `scope_id = 'context'` per run); under this proposal they keep the module and position, and only the Learn view collapses them. The witan task on keeping overview/summary sequentials' module and position covers that change, and the assumed-knowledge-in-overviews task depends on it.

### The platform crosswalk is a seed, not pattern matching

A seed `courseware_composition_crosswalk` (platform, native type or rule name, level, role default, basis) drives a `composition_level`, `role` and `level_basis` column on lessons, items and evidence locators. Cross-platform queries read those columns and never match native names.

| canonical | Open edX (OLX / Studio) | OCW | Canvas | standalone video | MIT Learn |
|---|---|---|---|---|---|
| course run | course / Course (native) | the site (native) | course shell (native) | implicit | `LearningResourceRun` |
| module | chapter / Section (native) | page whose children are lessons, in a tree deeper than two levels (rule, rare) | module whose SubHeaders delimit lessons (override) | none | none (flattened) |
| lesson | sequential / Subsection (native default) | Scholar week or part; SES/LEC/WEEK table row; top-level section page; orphan page (rule) | module (native default) | implicit, one per video | `Unit(kind=unit)` |
| segment | vertical / Unit (native) | child page of a lesson page (rule) | items under one SubHeader (rule) | none | none (flattened) |
| item | leaf block / Component (native) | resource: video, document, page (native) | module item (native) | the video | a block's own `ContentFile` (relation `core` or `assessment`); transcripts and `supplementary` static files are assets of it |

Per-platform rules from the vocabulary document, kept here so they have one home:

- Open edX: `library_content` and `itembank` are randomized pools (one progress item worth `max_count`). Extraction reads the first variant of a `library_content` pool; it has no rule for `itembank` yet, so every itembank variant is read, which needs fixing before the backfill. `conditional` and `split_test` are composition rules. Staff-only and track-restricted blocks are visibility rules. A sequential takes role `context` when it is titled "Module Overview and Learning Goals" wherever it sits, or is its chapter's only sequential and its title starts a word with intro, overview, welcome, conclusion, summary or wrap (Learn's `_is_context_sequential` rule, which `int__courseware__extraction_sources` copies; it is English-only, see the task on detecting context roles without English-title matching). A sequential is a lesson by convention only: Studio lets an author make one a whole week; the UAI courses follow the convention, others are unmeasured.
- OCW: no fixed hierarchy, so every level below the run has basis `rule`. Course description and syllabus pages are `context`. Documents no page links are `reference`. A textbook section is `reference` when the course teaches through three or more page groups and is read as lessons when it has fewer. A calendar row that announces a problem set places that set's documents in its lesson. Answer keys are excluded before any text is read.
- Canvas: assignments and quizzes are items of kind `problem` with role `assessment` by rule. Module requirements, prerequisites, release dates and mastery paths are composition rules. Not yet checked against real Canvas exports (no dbt model carries Canvas module structure since the canvas code location was retired in #3079).
- MIT Learn: `Unit(kind=bin)` is not a level; its files are items or assets with role `reference` and no lesson, which a Learn view may bin. `UnitContentFile.relation` (`core`, `assessment`, `supplementary`, `reference`) maps to role on membership; `supplementary` is an asset. Learn marks every problem `assessment`, so it has no practice/assessment split yet. Consumer contracts keep their consumer's names (`integrations__learn__content_file_units` included); the crosswalk maps them.

### Terms we stop using

`unit` (it means a vertical in the progress models, a sequential in `int__courseware__extraction_sources`, an OCW grouping, and a lesson in Learn), `subsection`, `section`, `component`, `scope`, and "course" for a delivery. Our renames: `scope_kind = 'unit'` becomes `composition_level = 'lesson'`, `scope_kind = 'context'` becomes `role = 'context'`, `scope_kind = 'materials'` becomes `role = 'reference'`. `content_units_completed` and `content_units_total` keep their names (ol-analytics-api exposes them) and the glossary defines them as counting segments.

## 5. Relations

Relations fall into families that must not share a predicate, because they answer different questions. The POC's `broader` combines kind-of and part-of, and its `builds_on` is a course-sequence observation, not a dependency (design document, "Relationships between concepts").

| family | relation | between | published as | produced by |
|---|---|---|---|---|
| containment | has part / is part of | content levels | `schema:hasPart` | structure |
| hierarchy | broader (kind of) | knowledge to knowledge | `skos:broader` + `iso-thes:broaderGeneric` | adjudication, Wikidata P279 candidates |
| hierarchy | part of | knowledge to knowledge | `skos:broader` + `iso-thes:broaderPartitive` | adjudication, Wikidata P361/P527 candidates |
| equivalence | exact (merged), close | entry to entry, entry to external vocabulary | `skos:exactMatch`, `skos:closeMatch` | resolution |
| association | related, complements (co-taught or co-assessed) | entry to entry | `skos:related`, `mitol:complements` | adjudication, course evidence |
| sequence | builds on (observed), practiced with (tool) | entry to entry | not published | course evidence plus support rule |
| dependency | requires concept | knowledge to knowledge | `mitol:requiresConcept` | review only |
| skill | exercises | skill to knowledge | `mitol:exercises` | text stage |
| teaching | teaches, reviews, uses, mentions | lesson or item to entry | `schema:teaches` (teaches only), `mitol:reviews`, `mitol:uses`, `mitol:mentions` | text stage |
| assessment | assesses | problem to entry | `schema:assesses` | text stage |
| requirement | competency required, course prerequisite, assumes | course or lesson to entry or course | `schema:competencyRequired`, `schema:coursePrerequisites`, `mitol:assumes` | text stage (declared, assumed); rule (candidates, unpublished) |
| people | instructor; presents; author; about | course run to person; person to lesson; work to person; lesson or work to person | `schema:instructor`, `mitol:presents`, `schema:author`, `schema:about` | catalog (instructor); entities facet (the rest) |
| cross-reference | refers to | lesson to lesson or course | `mitol:refersTo` | knowledge facet |

`iso-thes:` is the ISO 25964 SKOS extension (http://purl.org/iso25964/skos-thes#). "Adjudication" is the resolver's batched model verdict step (design document section 10).

CASE 1.1 associations (`isChildOf`, `isPartOf`, `isPeerOf`, `precedes`, `exactMatchOf`) and CTDL (`ceterms:teaches`, `requires`, `isPreparationFor`) remain export targets, mapped from these (both are described in the design document's vocabularies section).

## 6. What the text stage records, so content is read once

The test for a text-stage field: it needs the source text to decide (so it cannot be derived from stored claims and structure), a use case in section 2 reads it, and it can carry verbatim evidence. Everything that passes goes into the next schema version together. Everything that fails is derived downstream and listed at the end of this section.

### Kept from courseware_claims_v2

Summary, concepts, skills, assumed knowledge, assessment expectations, learning outcomes, declared prerequisites, audience, notes; the context stage's teaching-status revisions and sense hints.

### Added to the knowledge facet as courseware_claims_v3

- `ConceptClaim.treatment`: the set from section 3 (`intuitive`, `formal`, `computational`), required when the status is taught or reviewed.
- `ConceptClaim.examples`: the worked examples or cases the lesson uses to teach the term, as short phrases ("predicting house prices"). Published as `skos:example`; it also helps sense assignment and Learn concept pages.
- `ConceptClaim.version_mentioned`: for tools, the version the material names, else null. This is the currency signal for tools.
- `SkillClaim.concept_labels` and `SkillClaim.bloom_process`: the knowledge the skill acts on and the cognitive process (the `exercises` relation).
- `AssessmentClaim.skill_labels`: the skills the item requires, beside its `concept_labels`. Attestations read this.
- `MisconceptionClaim` (new kind): an error the material itself names as common ("a common mistake is to..."), with the concept labels it concerns. For AskTIM's tutor and for assessment design. Only stated misconceptions, never inferred ones.
- `CurrencyClaim` (new kind): a statement that dates the material ("as of 2021", "the current version", "the latest model"), with the year when it gives one.
- `CrossReferenceClaim` (new kind): the lesson points to another lesson or course by name ("as we saw in the regression lecture", "you will use this in UAI.4"). A lesson that leans on another cannot be lifted into a B2B package alone; the `refersTo` relation records that.
- `MaterialRequirementClaim` (new kind): what the material says is required, graded or optional (Learn's `required_materials`). Learn's `Conflict` (two quoted statements that disagree) needs no field: it is derived by comparing stored quoted statements across a run's context lessons.

### A second text-stage facet for people, organizations and works

People, organizations and cited works are a different task from concept extraction (name finding and role classification over the same text), and adding them to the knowledge call risks lowering concept recall. The v1 to v2 change alone moved the taught-concept count from 554 to 526 on the same runs. I propose a second facet call per lesson, `courseware_entities_v1`, keyed like the text stage (its own prompt and schema version plus the same message digest). It runs in the same pass over the same assembled text, so the corpus is processed in one backfill, and a later change to one facet re-runs only that facet. The cost is input tokens: each lesson's text is sent twice, roughly doubling input (v2 read 767k input tokens for the five UAI runs). The GLiNER2 option below could cut the entities facet to the lessons where a mention was found.

- `PersonMention`: name as written; `role_in_material` (`instructor`: presents the lesson; `guest`: a guest lecturer or interviewee; `author`: wrote a reading or work the lesson uses; `subject`: discussed, e.g. Thomas Bayes; `other`); stated title and affiliation when the material gives them; evidence. Instructors and guests resolve against `dim_instructor` through `bridge_course_instructor` first, since the catalog already holds names per platform; subjects resolve against Wikidata. That bridge is course-level and covers MITx Online, xPRO and OCW only (OCW instructors have no bios, xPRO's no titles), so edX.org, residential and Canvas runs have no catalog person to resolve against. For those, the mention itself is the only record of who presents a lesson until a catalog source exists.
- `OrganizationMention`: name; `org_kind` (`company`, `university`, `government`, `nonprofit`, `other`); `role_in_material` (`case_subject`, `data_source`, `partner`, `affiliation`, `vendor`); evidence. Industry is not asked for: it comes from Wikidata (section 7) once the organization is linked.
- `WorkReference`: title, authors, year, `work_kind` (`book`, `paper`, `article`, `video`, `website`, `software`, `dataset`), `citation_role` (`required_reading`, `optional_reading`, `cited`, `example_source`), and any identifier the text states (DOI, ISBN, URL). DOIs and URLs are also pulled by regex at assembly time, which costs nothing and checks the model's recall.

Learner content is out of scope by construction: Open edX lessons read only html, problem and video blocks (`int__courseware__extraction_sources`), so discussion posts never reach a model, and OCW has no learner content. Names of course staff who are not public instructors (TAs in a recitation video) are stored as observations like any other claim; whether they are published is a review-policy decision (section 9).

People and organization mentions can also be found without a model (GLiNER2, Apache-2.0, is in the design document's tooling list). If a GLiNER2 pass finds the same mentions on the five UAI runs, the model only has to classify roles, which is cheaper; that comparison belongs in the v3 evaluation.

### Derived downstream, not extracted

- Course level or difficulty: from Bloom processes, treatment depth and assumed knowledge across a run's lessons.
- Occupations and careers: from skills through ESCO and O*NET crosswalks.
- Industry of a case study: from the organization's Wikidata industry.
- Topics and course summaries: from resolution, clustering and the stored lesson summaries.
- Outline (Learn's `instructional_outline`): from structure and roles. Conflicts: from stored quoted statements, as above.
- Language of the material: from course metadata or a language identifier at assembly time, not a model call.
- Lesson activity type (lecture, worked example, coding exercise, lab, project): from item kinds and roles first. Add it to the text stage only if the gold set shows structure cannot tell a worked example from a lecture.

### How the schema change is evaluated

Run the knowledge facet v3 and the entities facet v1 locally on the five UAI runs (reuse off), as the v2 comparison did: claim counts by kind and status beside v2, evidence verification rate, concept label overlap, tokens per facet. Promote only if v3's taught-concept count and evidence rate stay within the v1 to v2 range. Then add a handful of OCW and non-UAI runs before the backfill, since five courses from one curricular family cannot show whether `treatment` or `role_in_material` generalize.

## 7. Wikidata: what to take from it

### What was probed

On 2026-11-18 I queried Wikidata (the action API and WDQS) for 25 labels typical of the UAI extractions, seven MIT instructors, and the property catalog. The queries and their output are summarized on the witan task "Link concepts to Wikidata at the concept level"; the counts below are from that run.

### Findings

- Coverage: 23 of 25 labels matched an item by exact English label with more than five sitelinks. The two misses were label variants ("pandas", "eigenvalues and eigenvectors"), which an alias index covers.
- Ambiguity: "Python" matched eight such items (the language, a mythological serpent, a snake genus, a missile, people, a film, a disambiguation page). Linking needs the term's kind and sense, and `instance of` (P31) filters the candidates by type.
- Hierarchy: `subclass of` (P279) and `instance of` (P31) are present on most items, with `part of` (P361), `has part` (P527), `facet of` (P1269) and `uses` (P2283) on many. They are uneven: gradient descent is a subclass of "mathematical concept", and linear regression of "machine learning". Usable as candidate edges for the adjudicator, not as accepted hierarchy.
- Constraints: `different from` (P1889) and `opposite of` (P461) appear on several items (overfitting / underfitting, Python / CPython). These are cannot-merge pairs for resolution.
- Crosswalks: OpenAlex ID (P10283) on 20 of 23 matched items; ACM Classification Code (P2179) on 2; MSC (P3285) on 1; MathWorld (P2812) on 3; ESCO skill (P4644) only on Python. This matches the design document's finding that ESCO, ACM CCS and MSC crosswalks inside Wikidata are thin. The OpenAlex bridge is the exception.
- Multilingual labels: sitelinks (Wikipedia editions with an article on the item, a proxy for how many languages carry a label; labels per language were not counted) were printed for ten of the items and range from 18 to 75 (large language model 75, convolutional neural network 35). That gives labels in the languages of the 288 translated UAI runs measured on 2026-11-10, so a French or Japanese run's concepts can resolve to the same entries.
- History: `discoverer or inventor` (P61) and `time of discovery` (P575) on several (Bayes' theorem, Thomas Bayes, 1763; backpropagation, Seppo Linnainmaa, 1970). Useful on Learn concept pages and as `subject` people.
- Prerequisite: the `prerequisite` property (P4967) exists but none of the 23 items carries it. Wikidata is no source of concept dependencies.
- Courses: the property catalog has no OpenCourseWare, MITx or edX course id, so Wikidata does not help course identity.
- Aliases include junk (the large language model item lists "word salad machine"), so aliases are retrieval candidates only, never `altLabel`s we publish.
- Licensing: Wikidata's structured data is CC0 (https://www.wikidata.org/wiki/Wikidata:Licensing). Wikipedia article text is not, and we do not import it.
- Access: the API rate-limited the probe after about ten calls. The pipeline uses a local dump, as the Wikidata linking task already specifies.
- People: of seven MIT instructors probed (through the action API, since WDQS was throttled to one request a minute during an outage), the five senior research faculty all have items with MIT as employer, four with an ORCID and three with a Google Scholar id. A lecturer's name matched an item with no MIT employer, which may be another person, and a senior lecturer had no item at all. None of the seven carries an OpenAlex author id. Wikidata enriches faculty who publish; it cannot be the instructor registry, and a name match needs the employer to confirm it.

### What we take

1. A QID per vocabulary entry, linked to the opaque UUID with a match type, published as `schema:sameAs` and `skos:exactMatch` or `skos:closeMatch`. The UUID stays the identity (design document section 10).
2. Multilingual labels into `dim_concept_alias` with `alias_source = wikidata`, for cross-lingual resolution and Learn search.
3. P279 (kind-of) and P361/P527 (part-of) as candidate hierarchy edges, fed to adjudication with their source recorded. P31 is used only to filter link candidates by type.
4. P1889 and P461 as cannot-merge constraints in batched resolution, beside the model's `distinct` verdicts.
5. P10283 as the bridge to OpenAlex topics and works, the one crosswalk Wikidata carries densely.
6. QIDs for `subject` people, for catalog instructors whose item names MIT (or the course's partner) as employer, and for organizations, with ORCID for people and industry (P452) and ROR ids (P6782) for organizations where present. Organization coverage was not probed; measure it on the entities facet's first output before relying on it for B2B sector matching. `dim_instructor` stays the instructor registry.

We do not take descriptions as learner-facing text (we generate our own), and nothing from Wikidata is published without passing the same review gate as extracted data.

## 8. Where the ontology lives

The design document proposes LinkML as the single schema that generates the extraction's Pydantic models, JSON Schema, DDL and a JSON-LD context. Today the Pydantic models in `ml.lib.courseware.schemas` are hand-written and the enumerations are repeated in dbt `accepted_values` tests and in Learn. I propose that the enumerations in this document (levels, roles, item kinds, term types, knowledge kinds, teaching status, treatment, relation predicates and their URIs) live in one LinkML schema under `src/ol_dbt` or `packages/`, generating dbt seeds and the `mitol:` JSON-LD context, while the extraction models stay hand-written until a second consumer needs generated code. That keeps the published vocabulary in one file without rewriting the extractor first. The human-readable definitions go into the open-learning business glossary in OpenMetadata (ol-infrastructure `om_server_config.py`, which already holds Course, Course run and Program), and a SKOS export of the LinkML schema is what OpenMetadata's `importRdf` would load if that route is adopted (the OpenMetadata SKOS import evaluation task).

## 9. Decisions for the team

Each is the choice itself and my recommendation.

1. Adopt the six levels (course, course run, module, lesson, segment, item), the four non-levels and the five roles as defined in section 4. Recommend yes; on 2026-11-10 Learn's lessons matched ours on the five UAI runs (41 of 41, same ids, titles and order).
2. Make skill a term type rather than a concept kind, and keep `dataset` as a kind. Recommend yes; it means a change to Learn's `ConceptOutput.kind`, which Learn's team needs to agree.
3. Treat prerequisite as the four assertions in section 3, with only review producing `requiresConcept`. Recommend yes; it is what the publication contract already says.
4. Add `treatment` depth, examples, tool versions, skill links, misconceptions, currency, cross-references and material requirements to the knowledge facet in one schema version. Recommend yes, evaluated on the five UAI runs plus a few OCW runs before any backfill.
5. Put people, organizations and works in a second facet call in the same pass, rather than in the knowledge call. Recommend the second facet, for concept recall and for independent versioning; the cost is one more call per lesson and roughly double the input tokens.
6. Store staff names that are not public instructors as observations, and publish only people who are catalog instructors or `subject` people with a Wikidata match. Recommend yes until someone owns a wider policy.
7. Split `broader` into kind-of and part-of before review starts, so reviewers judge the right predicate. Recommend yes.
8. Canvas modules default to lessons (override to module); OCW child pages are segments of their parent lesson. Recommend both, with the Canvas one confirmed against real exports when Canvas content returns.
9. Choose one extension prefix: `mitol:` (used in the design document) or `mitlocal:` (used in the vocabulary document). Recommend `mitol:` with a namespace URI under an MIT domain we control.
10. Keep the enumerations and predicate URIs in one LinkML schema that generates seeds and the JSON-LD context (section 8). Recommend yes.

## 10. What changes once this is agreed

- dbt: the crosswalk seed; `composition_level`, `role` and `level_basis` on lessons, items and evidence; the scope_kind renames; the glossary entries in OpenMetadata.
- Extraction: `courseware_claims_v3` and `courseware_entities_v1`, compared on the pilot runs, then one allowlist promotion and one backfill. Before the backfill, `int__courseware__extraction_sources` needs a first-variant rule for `itembank` pools like the one for `library_content`.
- Resolution: Wikidata linking moves ahead of the backfill's resolution run, since cannot-merge constraints and multilingual labels change what it merges.
- Learn: the mapping in section 4 (Learn's own models keep their names) and the `kind` change in decision 2.
- The annotation guideline: levels, roles, treatment and the entity facet get guideline sections before the gold set is labeled.
