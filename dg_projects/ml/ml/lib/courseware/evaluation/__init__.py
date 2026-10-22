"""Human gold set for courseware extraction and resolution.

Five annotation sets, each sampled deterministically by seed, exported as Label
Studio tasks, and scored from Label Studio's JSON export:

- claims: emitted claims, stratified by course family, extractor model and
  cross-model agreement (both models, one model only), for precision.
- subsections: unit scopes annotated exhaustively (every concept and its teaching
  status), stratified by course family and size, part of them blind to model
  output, for recall.
- label pairs: pairs the resolver merged, adjudicated pairs by verdict, and
  near-neighbour pairs it never saw, for merge precision and recall.
- clusters: resolved concepts stratified by size, for B-cubed.
- prerequisite pairs: inferred builds_on/practiced_with edges and co-occurring
  non-edges, three-way labels from two or three annotators, for precision, recall
  and kappa.

Precision and recall are estimated with stratified prediction-powered inference
(metrics.stratified_ppi), using model outputs as the autorater on the unlabelled
rest of each stratum.
"""
