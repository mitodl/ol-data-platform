# Business glossary

Each file here is one OpenMetadata glossary and its terms: what the words used
about learners, courses and enrollments mean in the warehouse, and the privacy
categories its columns fall into. The files are the source of truth, and
OpenMetadata is made to match them. Today there is one, `open-learning.yaml`.

## Format

```yaml
glossary:                       # OpenMetadata CreateGlossary fields
  name: open-learning
  displayName: Open Learning
  description: >-
    What the terms used about MIT Open Learning's learners ...
  mutuallyExclusive: false
  owners: [{type: team, name: data-platform}]     # resolved to ids on sync
  reviewers: [{type: team, name: data-platform}]
terms:                          # OpenMetadata CreateGlossaryTerm fields
- name: identifiability
  displayName: Identifiability
  description: How directly a column identifies a person, ...
  mutuallyExclusive: true       # a column carries at most one child term
  references:
  - {name: NIST SP 800-122, endpoint: https://nvlpubs.nist.gov/...}
- name: direct-identifier
  parent: identifiability       # a term declared above this one
  displayName: Direct identifier
  synonyms: [direct PII]
  description: ...
  relatedTerms: [pii]           # terms declared above this one
```

Terms are created in file order, so a term's `parent` and each of its
`relatedTerms` must be declared above it. OpenMetadata stores a related-term
link on both terms, so declare it once, on the later term. Term names are
unique across a glossary, ignoring case, and cannot contain a dot.

A term's fully qualified name is the glossary, its ancestors and its name:
`open-learning.identifiability.direct-identifier`. That is what dbt columns
use, so renaming a term or moving it under another parent means changing every
column that names it in the same PR.

Descriptions are Markdown. In a folded (`>-`) block a single blank line is a
line break, so leave two blank lines between paragraphs.

## What sync changes

`ol-dbt glossary sync` creates the glossary and any term OpenMetadata is
missing. After that it puts these fields back to the file wherever they
differ, overwriting an edit made in the OpenMetadata UI:

- the glossary's `description`;
- each term's `description`, `synonyms`, `references` and `relatedTerms`.

`displayName`, `mutuallyExclusive` and the glossary's `owners` and `reviewers`
are set only when sync creates the glossary or term. Every bot is denied
EditDisplayName, and OpenMetadata ignores a change to `mutuallyExclusive` once
the glossary or term exists. Change those in OpenMetadata as well as here.

Sync never sets a term's status. The glossary's reviewers (the `data-platform`
team) approve terms in OpenMetadata. The sync bot is not a reviewer, so
OpenMetadata creates each term In Review and puts a term back In Review, with
an approval task for the reviewers, when sync patches it. Rejecting that task
rolls the change back until the next sync, so a change a reviewer disagrees
with should be reverted here too.

Sync leaves alone a glossary or term a steward soft-deleted, a missing term
whose parent is missing or soft-deleted, and a missing term whose name the
glossary already holds under another parent (one moved in the UI). It reports
each as `skipped`. A failure on one term (e.g. a related term OpenMetadata does
not have) is reported as `failed`, the rest still sync, and the command exits
non-zero.

## Commands

```bash
# Publish to OpenMetadata. Needs OM_SERVER_URL and OM_BOT_JWT_TOKEN.
# --dry-run reads the catalog and writes nothing.
ol-dbt glossary sync --dry-run
ol-dbt glossary sync --glossary-dir glossary
```
