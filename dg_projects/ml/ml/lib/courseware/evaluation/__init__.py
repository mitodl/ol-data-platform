"""Human gold set for courseware extraction and resolution.

Five annotation sets, each sampled deterministically by seed, exported as Label
Studio tasks, and scored from Label Studio's JSON export:

- claims: emitted claims, stratified by discipline (a declared discipline map, or
  the course family), extractor model and cross-model agreement (both models, one
  model only), for precision, with label granularity errors reported beside it.
- subsections: unit scopes annotated exhaustively (every concept with its teaching
  status and salience, and the subsection's central objectives with the concepts
  its central explanation needs), stratified by discipline and size, part of them
  blind to model output, for recall.
- label pairs: pairs the resolver merged, adjudicated pairs by verdict, and
  near-neighbour pairs it never saw, for merge precision and recall.
- clusters: resolved concepts stratified by size, for B-cubed.
- prerequisite pairs: inferred builds_on/practiced_with edges and co-occurring
  non-edges. Hierarchy and dependency are separate judgments, each with
  context-dependent and uncertain outcomes, from two or three annotators, for
  precision, recall and kappa per predicate.

A sample from one course family (the five UAI courses) is an engineering fixture,
not cross-domain validation; declare disciplines to sample a sense-diverse
population.

Precision and recall are estimated with stratified prediction-powered inference
(metrics.stratified_ppi), using model outputs as the autorater on the unlabelled
rest of each stratum.
"""
