"""Populations and stratified samples for the four gold-set annotation sets.

Each sampler returns the whole population (every item, its stratum and its
autorater prediction, which stratified PPI needs for the unlabelled items) and the
sample to annotate, with the text an annotator sees. Sampling is deterministic: each
stratum draws from its own generator seeded by (seed, set, stratum), over items
sorted by id, so adding a stratum does not reshuffle the others.
"""

import hashlib
import json
import math
import re
from collections import defaultdict
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
from ml.lib.courseware.evaluation.matching import LabelMatcher, concept_label_key
from ml.lib.courseware.evaluation.metrics import MIN_LABELS_PER_STRATUM, allocate
from ml.lib.courseware.scopes import build_scopes

# Claim kinds whose label names a concept, so two models' claims can be matched.
CONCEPT_CLAIM_KINDS = ("concept", "assumed_knowledge", "declared_prerequisite")
# Disagreement strata are where the autorater is least reliable, so they get more
# labels per item than proportional allocation would give them.
DISAGREEMENT_OVERSAMPLE = 2.0
CLAIMS_MIN_PER_STRATUM = 20
# Characters of source text shown around a claim's evidence.
EVIDENCE_CONTEXT_CHARS = 1_500
# Unit scopes below this many characters are "short"; above, "long". The POC
# median is about 16k characters (a lecture with its transcript).
SHORT_SUBSECTION_CHARS = 20_000
# Blind and prefilled halves are estimated separately, so each needs two labels.
SUBSECTIONS_MIN_PER_STRATUM = 2 * MIN_LABELS_PER_STRATUM
BLIND_SHARE = 0.5
# Resolver verdicts that merge are the costly mistakes; oversample them.
PAIR_VERDICT_WEIGHTS = {
    "merged": 6.0,
    "exact": 3.0,
    "close": 3.0,
    "broader": 2.0,
    "narrower": 2.0,
    "related": 1.0,
    "distinct": 1.0,
    "unblocked": 0.05,
    "unadjudicated": 1.0,
}
PAIRS_MIN_PER_STRATUM = 20
# Near-neighbour pairs the resolver never adjudicated, by embedding rank, measure
# what blocking missed. Recall of merges is relative to this neighbourhood.
UNBLOCKED_MAX_RANK = 20
CLUSTER_NEIGHBOURS = 8
MULTI_MEMBER_OVERSAMPLE = 20.0
CLUSTERS_MIN_PER_STRATUM = 10
SEQUENCE_RELATIONS = ("builds_on", "practiced_with")
PREREQUISITES_MIN_PER_STRATUM = 10
STATUS_ORDER = ("taught", "reviewed", "used", "assumed", "mentioned")
_COURSE_NUMBER_SEPARATOR = re.compile(r"[.\-_ ]")


def course_family(courserun_readable_id: str) -> str:
    """Return the department-level family of a course run.

    "course-v1:UAI_SOURCE+UAI.4+1T2026" -> "UAI", "course-v1:MITx+6.00.1x+2T2026"
    -> "6". A run id without "+" parts is its own course number.

    :param courserun_readable_id: the run's readable id
    :returns: the course number's prefix before the first separator
    :rtype: str
    """
    parts = courserun_readable_id.split("+")
    number = parts[1] if len(parts) > 1 else courserun_readable_id
    return _COURSE_NUMBER_SEPARATOR.split(number)[0]


def model_family(model_id: str) -> str:
    """Return a short model name, "us.anthropic.claude-haiku-5-5" -> "haiku"."""
    match = re.search(r"claude-([a-z]+)", model_id)
    return match.group(1) if match else model_id


def _rng(seed: int, *parts: str) -> np.random.Generator:
    digest = hashlib.sha256("|".join([str(seed), *parts]).encode()).digest()
    return np.random.default_rng(int.from_bytes(digest[:8], "big"))


def _unit(seed: int, *parts: str) -> float:
    """Return a number in [0, 1) fixed by the seed and the parts (e.g. an item id)."""
    digest = hashlib.sha256("|".join([str(seed), *parts]).encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


SMALL_STRATA = "small_strata"
# What score.py estimates each set over, one value per subpopulation: every stratum,
# merged ones included, must lie wholly inside or outside each of them, or one can
# get fewer than MIN_LABELS_PER_STRATUM of the stratum's labels. Claims are scored
# per model; subsections per model over the scopes it extracted, so a stratum keeps
# to one set of models; pair precision is over merged pairs and prerequisite
# precision over edges.
SCORING_GROUPS: dict[str, pl.Expr | None] = {
    "claims": pl.col("model"),
    "subsections": pl.col("models"),
    "pairs": pl.when(pl.col("merged"))
    .then(pl.lit("merged"))
    .otherwise(pl.lit("unmerged")),
    "clusters": None,
    "prerequisites": pl.when(pl.col("edge_direction") == "none")
    .then(pl.lit("non_edge"))
    .otherwise(pl.lit("edge")),
}

# Coarser stratifications, finest first, for when the floors of the set's own do
# not fit the budget: each course family adds a full set of strata, so a corpus of
# many families pools them. Each keeps its SCORING_GROUPS column.
STRATA_FALLBACKS: dict[str, tuple[pl.Expr, ...]] = {
    "claims": (pl.format("{}|{}", "model", "agreement"),),
    "subsections": (pl.format("{}|{}", "size_band", "models"), pl.col("models")),
}


def coarsen_strata(
    population: pl.DataFrame,
    min_size: int = MIN_LABELS_PER_STRATUM,
    within: pl.Expr | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Merge strata too small to estimate, and drop a merged one still too small.

    A stratum needs MIN_LABELS_PER_STRATUM labels for its variance, so a stratum
    with fewer items can never be estimated. Every such stratum joins SMALL_STRATA,
    or with `within` the "small_strata|<group>" of its group (any partition is a
    valid stratification); a merged stratum still below min_size leaves the
    population with its items, and the estimates exclude them.

    :param population: items with a stratum column
    :param min_size: smallest stratum kept as is
    :param within: the scoring group of an item (SCORING_GROUPS), constant within
        each stratum; small strata only merge within a group
    :returns: the population with small strata merged, and the dropped strata
        with their sizes
    :rtype: tuple[pl.DataFrame, pl.DataFrame]
    """
    merged_name = (
        pl.lit(SMALL_STRATA)
        if within is None
        else pl.format("{}|{}", pl.lit(SMALL_STRATA), within)
    )
    population = population.with_columns(merged_name.alias("_merged_stratum"))
    sizes = population.group_by("stratum", "_merged_stratum").agg(
        pl.len().alias("population")
    )
    small = sizes.filter(pl.col("population") < min_size)
    too_small = (
        small.group_by("_merged_stratum")
        .agg(pl.col("population").sum())
        .filter(pl.col("population") < min_size)["_merged_stratum"]
        .implode()
    )
    population = population.with_columns(
        pl.when(pl.col("stratum").is_in(small["stratum"].implode()))
        .then(pl.col("_merged_stratum"))
        .otherwise(pl.col("stratum"))
        .alias("stratum")
    )
    return (
        population.filter(~pl.col("stratum").is_in(too_small)).drop("_merged_stratum"),
        small.filter(pl.col("_merged_stratum").is_in(too_small))
        .select("stratum", "population")
        .sort("stratum"),
    )


def fit_strata(
    population: pl.DataFrame, set_name: str, budget: int, min_size: int
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Coarsen a set's strata, falling back to coarser ones until the floors fit.

    The first stratification whose strata, once coarsened, need at most `budget`
    labels at min_size each (or all of a smaller stratum) is used, so a corpus with
    many course families pools them (STRATA_FALLBACKS). If even the coarsest does
    not fit, it is returned as is and allocate raises for the budget.

    :param population: items with a stratum column
    :param set_name: the gold set, a key of SCORING_GROUPS
    :param budget: labels the set will allocate
    :param min_size: smallest stratum kept as is, and the floor per stratum
    :returns: the coarsened population and the dropped strata, as coarsen_strata
    :rtype: tuple[pl.DataFrame, pl.DataFrame]
    """
    fallbacks = STRATA_FALLBACKS.get(set_name, ())
    for stratification in (pl.col("stratum"), *fallbacks):
        coarsened, dropped = coarsen_strata(
            population.with_columns(stratification.alias("stratum")),
            min_size,
            SCORING_GROUPS[set_name],
        )
        sizes = coarsened.group_by("stratum").len()["len"]
        if sizes.clip(upper_bound=min_size).sum() <= budget:
            break
    return coarsened, dropped


def stratified_sample(  # noqa: PLR0913
    population: pl.DataFrame,
    allocation: dict[str, int],
    *,
    seed: int,
    set_name: str,
    id_column: str = "item_id",
    stratum_column: str = "stratum",
) -> pl.DataFrame:
    """Draw allocation[stratum] items from each stratum without replacement."""
    drawn = []
    for stratum, count in sorted(allocation.items()):
        members = population.filter(pl.col(stratum_column) == stratum).sort(id_column)
        if count > members.height:
            msg = f"{set_name}/{stratum}: {count} wanted, {members.height} exist"
            raise ValueError(msg)
        rows = _rng(seed, set_name, stratum).choice(
            members.height, size=count, replace=False
        )
        drawn.append(members[sorted(rows.tolist())])
    return pl.concat(drawn) if drawn else population.clear()


@dataclass(frozen=True)
class GoldInputs:
    """Everything the samplers read, loaded from local parquet outputs.

    :param claims: current claims of every extractor model, one row per claim, with
        the extraction's scope and model columns
    :param evidence: evidence rows of those claims
    :param sources: int__courseware__extraction_sources rows
    :param assignments: concept assignment of every label key, newest run
    :param registry: concept registry, newest run
    :param decisions: resolver verdicts, newest per label pair across runs
    :param labels: int__courseware__concept_labels rows
    :param embeddings: one embedding per label key
    :param concepts: dim_concept rows
    :param relations: bridge_concept_relation rows
    :param matcher: the frozen label matching rule
    """

    claims: pl.DataFrame
    evidence: pl.DataFrame
    sources: pl.DataFrame
    assignments: pl.DataFrame
    registry: pl.DataFrame
    decisions: pl.DataFrame
    labels: pl.DataFrame
    embeddings: pl.DataFrame
    concepts: pl.DataFrame
    relations: pl.DataFrame
    matcher: LabelMatcher


def _newest_run(table: pl.DataFrame, runs: pl.DataFrame) -> pl.DataFrame:
    last = runs.sort("started_at")["resolution_run_id"][-1]
    return table.filter(pl.col("resolution_run_id") == last)


def load_inputs(  # noqa: PLR0913
    extraction_dirs: list[Path],
    sources: Path,
    identity_dir: Path,
    concept_labels: Path,
    concepts: Path,
    relations: Path,
    label_maps: list[Path] | None = None,
) -> GoldInputs:
    """Load the extraction, resolution and dimensional outputs a gold set samples.

    :param extraction_dirs: directories holding courseware_claim_extraction,
        courseware_claim and courseware_claim_evidence, one or more models each
    :param sources: int__courseware__extraction_sources as parquet
    :param identity_dir: directory holding the resolution tables
    :param concept_labels: int__courseware__concept_labels as parquet
    :param concepts: dim_concept as parquet
    :param relations: bridge_concept_relation as parquet
    :param label_maps: parquet files with concept_label_key and
        canonical_label_key, extra equivalences for the matching rule (e.g. a
        resolution of two models' labels together)
    :returns: the loaded inputs
    :rtype: GoldInputs
    """
    extractions = pl.concat(
        [
            pl.read_parquet(directory / "courseware_claim_extraction.parquet")
            for directory in extraction_dirs
        ],
        how="diagonal_relaxed",
    ).unique("extraction_key", keep="first")
    current = (
        extractions.sort("extracted_at", descending=True)
        .unique(
            ["platform", "courserun_readable_id", "scope_id", "scope_part", "model_id"],
            keep="first",
        )
        .select(
            "extraction_key",
            "model_id",
            "prompt_version",
            "platform",
            "courserun_readable_id",
            "scope_id",
            "scope_kind",
            "scope_title",
            "scope_position",
            "scope_part",
        )
    )
    claims = (
        pl.concat(
            [
                pl.read_parquet(directory / "courseware_claim.parquet")
                for directory in extraction_dirs
            ],
            how="diagonal_relaxed",
        )
        .unique(["extraction_key", "claim_ordinal"], keep="first")
        .join(current, on="extraction_key")
    )
    evidence = (
        pl.concat(
            [
                pl.read_parquet(directory / "courseware_claim_evidence.parquet")
                for directory in extraction_dirs
            ],
            how="diagonal_relaxed",
        )
        .unique(["extraction_key", "claim_ordinal", "evidence_ordinal"], keep="first")
        .join(current.select("extraction_key"), on="extraction_key")
    )
    runs = pl.read_parquet(identity_dir / "courseware_resolution_run.parquet")
    assignments = _newest_run(
        pl.read_parquet(identity_dir / "courseware_concept_assignment.parquet"), runs
    )
    registry = _newest_run(
        pl.read_parquet(identity_dir / "courseware_concept_registry.parquet"), runs
    )
    run_order = runs.select(
        "resolution_run_id", pl.col("started_at").alias("run_started_at")
    )
    decisions = (
        pl.read_parquet(identity_dir / "courseware_resolution_decision.parquet")
        .join(run_order, on="resolution_run_id")
        .filter(pl.col("query_label_key") != pl.col("candidate_label_key"))
        .with_columns(
            pl.min_horizontal("query_label_key", "candidate_label_key").alias("key_a"),
            pl.max_horizontal("query_label_key", "candidate_label_key").alias("key_b"),
        )
        .sort("run_started_at", descending=True)
        .unique(["key_a", "key_b"], keep="first")
    )
    embeddings = (
        pl.read_parquet(identity_dir / "courseware_concept_label_embedding.parquet")
        .with_row_index("row")
        .sort("row", descending=True)
        .unique("concept_label_key", keep="first")
        .select("concept_label_key", "embedding")
    )
    matcher = LabelMatcher().add_assignments(assignments)
    for path in label_maps or []:
        mapping = pl.read_parquet(path)
        matcher.add_mapping(
            mapping.select("concept_label_key", "canonical_label_key").iter_rows()
        )
    return GoldInputs(
        claims=claims,
        evidence=evidence,
        sources=pl.read_parquet(sources),
        assignments=assignments,
        registry=registry,
        decisions=decisions,
        labels=pl.read_parquet(concept_labels),
        embeddings=embeddings,
        concepts=pl.read_parquet(concepts),
        relations=pl.read_parquet(relations),
        matcher=matcher,
    )


def _claim_status(claim_kind: str, teaching_status: str | None) -> str:
    """Teaching status for concept claims; the claim kind stands in for the rest."""
    if claim_kind == "concept":
        return teaching_status or "mentioned"
    return "assumed" if claim_kind == "assumed_knowledge" else claim_kind


def _with_canonical(inputs: GoldInputs) -> pl.DataFrame:
    return inputs.claims.with_columns(
        pl.format("{}:{}", "extraction_key", "claim_ordinal").alias("item_id"),
        pl.col("model_id")
        .map_elements(model_family, return_dtype=pl.String)
        .alias("model"),
        pl.col("courserun_readable_id")
        .map_elements(course_family, return_dtype=pl.String)
        .alias("course_family"),
        pl.when(pl.col("claim_kind").is_in(CONCEPT_CLAIM_KINDS))
        .then(
            pl.col("label").map_elements(
                inputs.matcher.canonical, return_dtype=pl.String
            )
        )
        .alias("canonical"),
        pl.struct("claim_kind", "teaching_status")
        .map_elements(
            lambda row: _claim_status(row["claim_kind"], row["teaching_status"]),
            return_dtype=pl.String,
        )
        .alias("status"),
    )


def claims_population(inputs: GoldInputs) -> pl.DataFrame:
    """Every current claim with its stratum and cross-model autorater prediction.

    agreement is "both" when another model that extracted the same scope part named
    the same concept, "<model>_only" when none did, "unpaired" when no other model
    extracted the scope, and "not_compared" for claim kinds without a concept label.
    The autorater is the other model: 1 when it also gave the concept the same
    status, 0 otherwise, and null where there is no other model or no concept.

    :param inputs: loaded gold-set inputs
    :returns: one row per claim with item_id, stratum, agreement and autorater
    :rtype: pl.DataFrame
    """
    claims = _with_canonical(inputs)
    scope = ["platform", "courserun_readable_id", "scope_id", "scope_part"]
    models_per_scope = claims.group_by(scope).agg(
        pl.col("model").unique().alias("scope_models")
    )
    named = (
        claims.filter(pl.col("canonical").is_not_null())
        .select(*scope, "model", "canonical", "status")
        .unique()
    )
    same_concept = (
        claims.select("item_id", *scope, "model", "canonical", "status")
        .join(named, on=[*scope, "canonical"], suffix="_other")
        .filter(pl.col("model") != pl.col("model_other"))
        .group_by("item_id")
        .agg((pl.col("status") == pl.col("status_other")).any().alias("same_status"))
    )
    claims = (
        claims.join(models_per_scope, on=scope)
        .join(same_concept, on="item_id", how="left")
        .with_columns(
            pl.when(pl.col("scope_models").list.len() < 2)  # noqa: PLR2004
            .then(pl.lit("unpaired"))
            .when(pl.col("canonical").is_null())
            .then(pl.lit("not_compared"))
            .when(pl.col("same_status").is_not_null())
            .then(pl.lit("both"))
            .otherwise(pl.format("{}_only", "model"))
            .alias("agreement")
        )
        .with_columns(
            pl.when(pl.col("agreement") == "both")
            .then(pl.col("same_status").cast(pl.Float64))
            .when(pl.col("agreement").str.ends_with("_only"))
            .then(pl.lit(0.0))
            .alias("autorater"),
            pl.format("{}|{}|{}", "course_family", "model", "agreement").alias(
                "stratum"
            ),
        )
    )
    return claims.drop("scope_models", "same_status").sort("item_id")


def _source_texts(sources: pl.DataFrame) -> dict[tuple[str, str], tuple[str, str]]:
    return {
        (row["courserun_readable_id"], row["source_ref"]): (
            row["source_title"] or row["source_ref"],
            row["source_text"],
        )
        for row in sources.select(
            "courserun_readable_id", "source_ref", "source_title", "source_text"
        )
        .unique(["courserun_readable_id", "source_ref"])
        .iter_rows(named=True)
    }


def _claim_window(
    claim: dict[str, Any],
    evidence: list[dict[str, Any]],
    texts: dict[tuple[str, str], tuple[str, str]],
) -> tuple[str, list[dict[str, Any]]]:
    """Source excerpts around a claim's evidence, with the evidence spans in them."""
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in evidence:
        if row["source_ref"] is not None:
            by_source[row["source_ref"]].append(row)
    pieces: list[str] = []
    regions: list[dict[str, Any]] = []
    offset = 0
    for source_ref, rows in by_source.items():
        title, text = texts[claim["courserun_readable_id"], source_ref]
        located = [row for row in rows if row["char_start"] is not None]
        start = max(
            0,
            min((row["char_start"] for row in located), default=0)
            - EVIDENCE_CONTEXT_CHARS,
        )
        end = min(
            len(text),
            max((row["char_end"] for row in located), default=0)
            + EVIDENCE_CONTEXT_CHARS,
        )
        header = f"=== {title} ===\n"
        excerpt = (
            ("..." if start else "")
            + text[start:end]
            + ("..." if end < len(text) else "")
        )
        base = offset + len(header) + (3 if start else 0) - start
        regions.extend(
            {
                "start": base + row["char_start"],
                "end": base + row["char_end"],
                "label": "evidence",
            }
            for row in located
        )
        piece = header + excerpt + "\n\n"
        pieces.append(piece)
        offset += len(piece)
    text = "".join(pieces)
    for region in regions:
        region["text"] = text[region["start"] : region["end"]]
    return text, regions


def sample_claims(
    inputs: GoldInputs, population: pl.DataFrame, *, size: int, seed: int
) -> pl.DataFrame:
    """Draw the claims to verify, with the claim and its evidence in context.

    :param inputs: loaded gold-set inputs
    :param population: claims_population output
    :param size: labels in total (300 to 500 per the design)
    :param seed: sampling seed
    :returns: sampled claims with source_text and evidence regions
    :rtype: pl.DataFrame
    """
    sizes = dict(population.group_by("stratum").len().iter_rows())
    weights = {
        name: DISAGREEMENT_OVERSAMPLE if name.endswith("_only") else 1.0
        for name in sizes
    }
    allocation = allocate(sizes, size, weights=weights, minimum=CLAIMS_MIN_PER_STRATUM)
    sample = stratified_sample(population, allocation, seed=seed, set_name="claims")
    texts = _source_texts(inputs.sources)
    evidence = defaultdict(list)
    for row in inputs.evidence.sort("evidence_ordinal").iter_rows(named=True):
        evidence[row["extraction_key"], row["claim_ordinal"]].append(row)
    titles = dict(
        inputs.sources.select("courserun_readable_id", "course_title")
        .unique("courserun_readable_id")
        .iter_rows()
    )
    rows = []
    for claim in sample.iter_rows(named=True):
        claim_evidence = evidence[claim["extraction_key"], claim["claim_ordinal"]]
        text, regions = _claim_window(claim, claim_evidence, texts)
        quotes = "\n".join(
            f'- "{row["passage"]}" ({row["verification_status"]})'
            for row in claim_evidence
        )
        rows.append(
            {
                "item_id": claim["item_id"],
                "stratum": claim["stratum"],
                "heading": (
                    f"{titles[claim['courserun_readable_id']]} / {claim['scope_title']}"
                ),
                "claim_text": (
                    f"{claim['claim_kind']}: {claim['label']}\n"
                    f"status: {claim['status']}\n"
                    f"{claim['description'] or ''}\n\nQuoted evidence:\n{quotes}"
                ),
                "source_text": text,
                "regions_json": json.dumps(regions),
            }
        )
    return pl.DataFrame(rows)


def _scope_concepts(claims: pl.DataFrame) -> pl.DataFrame:
    """Each (scope, model)'s distinct concepts with their strongest status."""
    rank = {status: index for index, status in enumerate(STATUS_ORDER)}
    return (
        claims.filter(pl.col("claim_kind").is_in(("concept", "assumed_knowledge")))
        .with_columns(
            pl.col("status")
            .replace_strict(rank, default=len(STATUS_ORDER))
            .alias("status_rank")
        )
        .group_by("platform", "courserun_readable_id", "scope_id", "model", "canonical")
        .agg(pl.col("status").sort_by("status_rank").first())
    )


def scope_concepts(inputs: GoldInputs) -> pl.DataFrame:
    """Return each model's distinct concepts per scope, with the strongest status.

    :param inputs: loaded gold-set inputs
    :returns: platform, courserun_readable_id, scope_id, model, canonical, status
    :rtype: pl.DataFrame
    """
    return _scope_concepts(_with_canonical(inputs))


def subsections_population(inputs: GoldInputs) -> pl.DataFrame:
    """Every unit scope with its stratum and per-model pooled-recall proxies.

    For model m, found_hat_<m> is how many distinct concepts m named in the scope
    and gold_hat is how many all models that extracted the scope named together. The
    ratio is the recall that model agreement suggests, which the human annotation
    corrects. A scope no model extracted is left out.

    :param inputs: loaded gold-set inputs
    :returns: one row per unit scope
    :rtype: pl.DataFrame
    """
    claims = _with_canonical(inputs)
    concepts = _scope_concepts(claims)
    scope = ["platform", "courserun_readable_id", "scope_id"]
    units = (
        inputs.sources.filter(pl.col("scope_kind") == "unit")
        .group_by(*scope, "scope_title", "scope_position")
        .agg(pl.col("source_text").str.len_chars().sum().alias("chars"))
    )
    models = sorted(claims["model"].unique())
    # a column per model that extracted anything, named a concept or not
    per_model = concepts.group_by(scope).agg(
        (pl.col("model") == model).sum().alias(f"found_hat_{model}") for model in models
    )
    pooled = concepts.group_by(scope).agg(
        pl.col("canonical").n_unique().alias("gold_hat")
    )
    extracted = claims.group_by(scope).agg(
        pl.col("model").unique().sort().str.join("+").alias("models")
    )
    return (
        units.join(extracted, on=scope)
        .join(pooled, on=scope, how="left")
        .join(per_model, on=scope, how="left")
        .with_columns(
            pl.col("gold_hat").fill_null(0),
            *[pl.col(f"found_hat_{model}").fill_null(0) for model in models],
            pl.format("{}#unit", "scope_id").alias("item_id"),
            pl.when(pl.col("chars") < SHORT_SUBSECTION_CHARS)
            .then(pl.lit("short"))
            .otherwise(pl.lit("long"))
            .alias("size_band"),
            pl.col("courserun_readable_id")
            .map_elements(course_family, return_dtype=pl.String)
            .alias("course_family"),
        )
        .with_columns(
            pl.format("{}|{}|{}", "course_family", "size_band", "models").alias(
                "stratum"
            )
        )
        .sort("item_id")
    )


def blind_items(sample: pl.DataFrame, seed: int) -> set[str]:
    """Choose the blind half of each stratum by a hash of seed and item id.

    Per stratum, so both halves can be estimated per stratum, and per item, so
    adding a stratum or an item elsewhere moves no flag.

    :param sample: sampled items with item_id and stratum
    :param seed: sampling seed
    :returns: the item ids annotated without pre-annotations
    :rtype: set[str]
    """
    blind = set()
    for (_,), members in sample.group_by("stratum"):
        ranked = sorted(
            members["item_id"], key=lambda item: _unit(seed, "subsections-blind", item)
        )
        blind.update(ranked[: math.ceil(len(ranked) * BLIND_SHARE)])
    return blind


def sample_subsections(
    inputs: GoldInputs, population: pl.DataFrame, *, size: int, seed: int
) -> pl.DataFrame:
    """Draw unit scopes for exhaustive annotation; half are blind to model output.

    The task text is every source of the scope in course order, as the extractor
    read it. Non-blind tasks carry the pooled concepts of every model as
    pre-annotations, placed at their evidence spans.

    :param inputs: loaded gold-set inputs
    :param population: subsections_population output
    :param size: subsections in total (20 to 40 per the design)
    :param seed: sampling seed
    :returns: sampled scopes with text, blind flag and pre-annotation regions
    :rtype: pl.DataFrame
    """
    sizes = dict(population.group_by("stratum").len().iter_rows())
    allocation = allocate(
        sizes,
        size,
        minimum=SUBSECTIONS_MIN_PER_STRATUM,
        required=SUBSECTIONS_MIN_PER_STRATUM,
    )
    sample = stratified_sample(
        population, allocation, seed=seed, set_name="subsections"
    )
    blind = blind_items(sample, seed)
    claims = _with_canonical(inputs)
    evidence = inputs.evidence.filter(pl.col("char_start").is_not_null())
    titles = dict(
        inputs.sources.select("courserun_readable_id", "course_title")
        .unique("courserun_readable_id")
        .iter_rows()
    )
    rows = []
    for unit in sample.iter_rows(named=True):
        run_sources = inputs.sources.filter(
            pl.col("courserun_readable_id") == unit["courserun_readable_id"]
        )
        scopes = [
            scope
            for scope in build_scopes(run_sources)
            if scope.scope_id == unit["scope_id"]
        ]
        pieces = []
        offsets: dict[str, int] = {}
        position = 0
        for scope in scopes:
            for source in scope.sources:
                header = f"=== {source.title} ({source.source_kind}) ===\n"
                offsets[source.source_ref] = position + len(header)
                piece = header + source.text + "\n\n"
                pieces.append(piece)
                position += len(piece)
        text = "".join(pieces)
        regions: list[dict[str, Any]] = []
        if unit["item_id"] not in blind:
            scope_claims = claims.filter(
                (pl.col("courserun_readable_id") == unit["courserun_readable_id"])
                & (pl.col("scope_id") == unit["scope_id"])
                & pl.col("claim_kind").is_in(("concept", "assumed_knowledge"))
            ).join(evidence, on=["extraction_key", "claim_ordinal"])
            seen = set()
            for claim in scope_claims.sort("item_id", "evidence_ordinal").iter_rows(
                named=True
            ):
                if claim["canonical"] in seen or claim["source_ref"] not in offsets:
                    continue
                seen.add(claim["canonical"])
                start = offsets[claim["source_ref"]] + claim["char_start"]
                end = offsets[claim["source_ref"]] + claim["char_end"]
                regions.append(
                    {
                        "start": start,
                        "end": end,
                        "text": text[start:end],
                        "label": claim["status"],
                        "concept": claim["label"],
                    }
                )
        rows.append(
            {
                "item_id": unit["item_id"],
                "stratum": unit["stratum"],
                "blind": unit["item_id"] in blind,
                "heading": (
                    f"{titles[unit['courserun_readable_id']]} / {unit['scope_title']}"
                ),
                "source_text": text,
                "regions_json": json.dumps(regions),
            }
        )
    return pl.DataFrame(rows)


def _label_info(inputs: GoldInputs) -> dict[str, dict[str, Any]]:
    return {
        row["concept_label_key"]: row for row in inputs.labels.iter_rows(named=True)
    }


def _unblocked_pairs(
    embeddings: pl.DataFrame, decided: set[tuple[str, str]]
) -> list[tuple[str, str]]:
    keys = embeddings["concept_label_key"].to_list()
    vectors = np.array(embeddings["embedding"].to_list(), dtype=np.float32)
    vectors /= np.linalg.norm(vectors, axis=1, keepdims=True)
    pairs = set()
    for start in range(0, len(keys), 1024):
        similarity = vectors[start : start + 1024] @ vectors.T
        for offset, row in enumerate(similarity):
            query = start + offset
            row[query] = -np.inf
            for neighbour in np.argsort(-row)[:UNBLOCKED_MAX_RANK]:
                pair = tuple(sorted((keys[query], keys[int(neighbour)])))
                if pair not in decided:
                    pairs.add(pair)
    return sorted(pairs)


def pairs_population(inputs: GoldInputs) -> pl.DataFrame:
    """Every adjudicated pair, merged pair and unblocked near-neighbour pair.

    A pair is merged when both labels are assigned to the same surviving concept,
    which is the resolver's output: an exact verdict merges only if its component
    passes the size and distinct checks, and merges made through other labels or in
    earlier runs count too. Merged pairs are their own stratum, and every other
    pair is stratified by its verdict ("unblocked" if never adjudicated). autorater
    is 1 for merged pairs. resolver_relation is the resolver's final relation of
    key_a to key_b: exact when merged, the verdict's relation otherwise, and
    distinct where nothing links the two (an unmerged exact verdict included).

    Only current labels (inputs.labels) pair up. The decision and embedding tables
    accumulate over runs and keep labels no current claim uses, which the samplers
    have no text for.

    :param inputs: loaded gold-set inputs
    :returns: one row per unordered label pair (key_a < key_b)
    :rtype: pl.DataFrame
    """
    concept_of = dict(
        _active_assignments(inputs)
        .select("concept_label_key", "concept_id")
        .iter_rows()
    )
    current = set(inputs.labels["concept_label_key"])
    decided_rows = inputs.decisions.filter(
        pl.col("key_a").is_in(current) & pl.col("key_b").is_in(current)
    ).select("key_a", "key_b", "query_label_key", "verdict")
    decided = set(decided_rows.select("key_a", "key_b").iter_rows())
    unblocked = set(
        _unblocked_pairs(
            inputs.embeddings.filter(pl.col("concept_label_key").is_in(current)),
            decided,
        )
    )
    members: dict[str, list[str]] = defaultdict(list)
    for key, concept in concept_of.items():
        members[concept].append(key)
    merged_only = {
        pair
        for keys in members.values()
        for pair in combinations(sorted(keys), 2)
        if pair not in decided and pair not in unblocked
    }
    extra = pl.DataFrame(
        [(*pair, pair[0], "unblocked") for pair in sorted(unblocked)]
        + [(*pair, pair[0], "unadjudicated") for pair in sorted(merged_only)],
        schema=["key_a", "key_b", "query_label_key", "verdict"],
        orient="row",
    )
    rows = []
    for pair in pl.concat([decided_rows, extra]).iter_rows(named=True):
        concept_a = concept_of.get(pair["key_a"])
        merged = concept_a is not None and concept_a == concept_of.get(pair["key_b"])
        if merged:
            relation = "exact"
        elif pair["verdict"] in ("exact", "unblocked", "unadjudicated"):
            relation = "distinct"
        else:
            relation = pair_relation(
                pair["verdict"], query_is_a=pair["query_label_key"] == pair["key_a"]
            )
        rows.append(
            {
                **pair,
                "item_id": f"{pair['key_a']}||{pair['key_b']}",
                "merged": merged,
                "stratum": "merged" if merged else pair["verdict"],
                "autorater": float(merged),
                "resolver_relation": relation,
            }
        )
    return pl.DataFrame(rows).sort("item_id")


_FLIPPED_RELATION = {"a_broader": "a_narrower", "a_narrower": "a_broader"}


def pair_relation(verdict: str, *, query_is_a: bool) -> str:
    """Return the resolver verdict as a relation of A to B."""
    relation = {"broader": "a_broader", "narrower": "a_narrower"}.get(verdict, verdict)
    return relation if query_is_a else _FLIPPED_RELATION.get(relation, relation)


def _label_text(info: dict[str, Any]) -> str:
    return (
        f"{info['preferred_label']} ({info['concept_kind']})\n"
        f"{info['description'] or ''}\n"
        f"named in {info['courserun_count']} course run(s), "
        f"{info['mention_count']} time(s)"
    )


def sample_pairs(
    inputs: GoldInputs, population: pl.DataFrame, *, size: int, seed: int
) -> pl.DataFrame:
    """Draw label pairs to judge, each shown in a seeded random order.

    :param inputs: loaded gold-set inputs
    :param population: pairs_population output
    :param size: pairs in total (a few hundred per the design)
    :param seed: sampling seed
    :returns: sampled pairs with both labels' text and the resolver's relation
        of A to B
    :rtype: pl.DataFrame
    """
    sizes = dict(population.group_by("stratum").len().iter_rows())
    allocation = allocate(
        sizes, size, weights=PAIR_VERDICT_WEIGHTS, minimum=PAIRS_MIN_PER_STRATUM
    )
    sample = stratified_sample(population, allocation, seed=seed, set_name="pairs")
    info = _label_info(inputs)
    rows = []
    for pair in sample.iter_rows(named=True):
        flip = _unit(seed, "pairs-orientation", pair["item_id"]) < 0.5  # noqa: PLR2004
        key_a, key_b = (
            (pair["key_b"], pair["key_a"]) if flip else (pair["key_a"], pair["key_b"])
        )
        rows.append(
            {
                "item_id": pair["item_id"],
                "stratum": pair["stratum"],
                "key_a": key_a,
                "key_b": key_b,
                "label_a": info[key_a]["preferred_label"],
                "label_b": info[key_b]["preferred_label"],
                "text_a": _label_text(info[key_a]),
                "text_b": _label_text(info[key_b]),
                "resolver_relation": _FLIPPED_RELATION.get(
                    pair["resolver_relation"], pair["resolver_relation"]
                )
                if flip
                else pair["resolver_relation"],
            }
        )
    return pl.DataFrame(rows)


def _active_assignments(inputs: GoldInputs) -> pl.DataFrame:
    """Return the current labels' assignments, merged ids followed to survivors.

    The newest run's assignments carry every label key resolution has seen, so a
    label no current claim uses (absent from inputs.labels) is left out.
    """
    redirects = {
        row["concept_id"]: row["merged_into_concept_id"]
        for row in inputs.registry.iter_rows(named=True)
        if row["merged_into_concept_id"] is not None
    }

    def survivor(concept_id: str) -> str:
        while concept_id in redirects:
            concept_id = redirects[concept_id]
        return concept_id

    return inputs.assignments.filter(
        pl.col("concept_label_key").is_in(set(inputs.labels["concept_label_key"]))
    ).with_columns(pl.col("concept_id").map_elements(survivor, return_dtype=pl.String))


def clusters_population(inputs: GoldInputs) -> pl.DataFrame:
    """Every resolved concept as a cluster of label keys, stratified by size.

    :param inputs: loaded gold-set inputs
    :returns: one row per concept with its member keys and size
    :rtype: pl.DataFrame
    """
    return (
        _active_assignments(inputs)
        .group_by("concept_id")
        .agg(pl.col("concept_label_key").sort().alias("members"))
        .with_columns(
            pl.col("members").list.len().alias("size"),
            pl.col("concept_id").alias("item_id"),
        )
        .with_columns(
            pl.when(pl.col("size") == 1)
            .then(pl.lit("singleton"))
            .otherwise(pl.lit("multi"))
            .alias("stratum")
        )
        .sort("item_id")
    )


def sample_clusters(
    inputs: GoldInputs, population: pl.DataFrame, *, size: int, seed: int
) -> pl.DataFrame:
    """Draw concepts to review, each with its nearest other concepts.

    The reviewer sorts the members into groups that are one concept each and puts a
    neighbour into a group when it is that concept too. Neighbours are the
    concepts nearest the cluster's mean label embedding.

    :param inputs: loaded gold-set inputs
    :param population: clusters_population output
    :param size: clusters in total (about 100 per the design)
    :param seed: sampling seed
    :returns: sampled clusters with list items for members and neighbours
    :rtype: pl.DataFrame
    """
    sizes = dict(population.group_by("stratum").len().iter_rows())
    allocation = allocate(
        sizes,
        size,
        weights={"multi": MULTI_MEMBER_OVERSAMPLE},
        minimum=CLUSTERS_MIN_PER_STRATUM,
    )
    sample = stratified_sample(population, allocation, seed=seed, set_name="clusters")
    info = _label_info(inputs)
    vectors = dict(inputs.embeddings.iter_rows())
    members = {
        row["item_id"]: row["members"] for row in population.iter_rows(named=True)
    }
    ids = sorted(members)
    centroids = np.array(
        [
            np.mean([vectors[key] for key in members[concept]], axis=0)
            for concept in ids
        ],
        dtype=np.float32,
    )
    centroids /= np.linalg.norm(centroids, axis=1, keepdims=True)
    index = {concept: position for position, concept in enumerate(ids)}
    labels = dict(inputs.concepts.select("concept_pk", "preferred_label").iter_rows())
    rows = []
    for cluster in sample.iter_rows(named=True):
        similarity = centroids @ centroids[index[cluster["item_id"]]]
        similarity[index[cluster["item_id"]]] = -np.inf
        neighbours = [ids[int(i)] for i in np.argsort(-similarity)[:CLUSTER_NEIGHBOURS]]
        items = [
            {
                "id": f"m:{key}",
                "title": info[key]["preferred_label"],
                "body": f"member | {info[key]['description'] or ''}",
            }
            for key in cluster["members"]
        ] + [
            {
                "id": f"n:{concept}",
                "title": labels[concept],
                "body": "other concept | labels: "
                + ", ".join(info[key]["preferred_label"] for key in members[concept]),
            }
            for concept in neighbours
        ]
        rows.append(
            {
                "item_id": cluster["item_id"],
                "stratum": cluster["stratum"],
                "heading": f"Concept: {labels[cluster['item_id']]}",
                "items_json": json.dumps(items),
                "neighbour_sizes_json": json.dumps(
                    {concept: len(members[concept]) for concept in neighbours}
                ),
            }
        )
    return pl.DataFrame(rows)


def prerequisites_population(inputs: GoldInputs) -> pl.DataFrame:
    """Concept pairs with a sequence edge, and co-occurring pairs without one.

    An edge (from, to) says `to` comes before `from`. A pair with edges both ways is
    stratum "bidirectional"; otherwise an edge pair's stratum is its relation and
    support band, and a non-edge pair's stratum says whether another relation links
    it (hierarchy, association) or none does. Pairs are unordered (concept_a <
    concept_b); edge_direction says which comes first, as the edges have it.

    :param inputs: loaded gold-set inputs
    :returns: one row per unordered concept pair
    :rtype: pl.DataFrame
    """
    relations = inputs.relations.filter(
        pl.col("from_concept_fk") != pl.col("to_concept_fk")
    )
    sequence: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    other: dict[tuple[str, str], set[str]] = defaultdict(set)
    for edge in relations.iter_rows(named=True):
        pair = tuple(sorted((edge["from_concept_fk"], edge["to_concept_fk"])))
        if edge["relation"] in SEQUENCE_RELATIONS:
            sequence[pair].append(edge)
        else:
            other[pair].add(edge["relation_family"])

    assigned = dict(
        _active_assignments(inputs)
        .select("concept_label_key", "concept_id")
        .iter_rows()
    )
    claims = _with_canonical(inputs).filter(
        pl.col("claim_kind").is_in(CONCEPT_CLAIM_KINDS)
    )
    per_run: dict[str, set[str]] = defaultdict(set)
    for run, label in claims.select("courserun_readable_id", "label").iter_rows():
        concept = assigned.get(concept_label_key(label))
        if concept is not None:
            per_run[run].add(concept)
    co_occurring = {
        pair
        for concepts in per_run.values()
        for pair in combinations(sorted(concepts), 2)
    }

    rows = []
    for pair in sorted(co_occurring | set(sequence)):
        edges = sequence.get(pair, [])
        directions = {edge["from_concept_fk"] for edge in edges}
        if len(directions) > 1:
            stratum, direction = "bidirectional", "both"
        elif edges:
            edge = max(
                edges,
                key=lambda e: (e["courserun_support"], e["relation"] == "builds_on"),
            )
            band = "support1" if edge["courserun_support"] < 2 else "support2+"  # noqa: PLR2004
            stratum = f"{edge['relation']}|{band}"
            # from builds on to: to comes first
            direction = (
                "b_before_a" if edge["from_concept_fk"] == pair[0] else "a_before_b"
            )
        else:
            families = other.get(pair, set())
            linked = (
                "hierarchy"
                if "hierarchy" in families
                else "associated"
                if families
                else "none"
            )
            stratum, direction = f"non_edge|{linked}", "none"
        rows.append(
            {
                "item_id": f"{pair[0]}||{pair[1]}",
                "concept_a": pair[0],
                "concept_b": pair[1],
                "stratum": stratum,
                "edge_direction": direction,
                "autorater": 0.0 if direction == "none" else 1.0,
            }
        )
    return pl.DataFrame(rows)


def sample_prerequisites(
    inputs: GoldInputs, population: pl.DataFrame, *, size: int, seed: int
) -> pl.DataFrame:
    """Draw half edge pairs and half non-edge pairs, each in a random order.

    :param inputs: loaded gold-set inputs
    :param population: prerequisites_population output
    :param size: pairs in total
    :param seed: sampling seed
    :returns: sampled pairs with both concepts' text and the edge direction in
        the shown order
    :rtype: pl.DataFrame
    """
    is_edge = pl.col("edge_direction") != "none"
    allocation: dict[str, int] = {}
    for half, budget in ((is_edge, size // 2), (~is_edge, size - size // 2)):
        sizes = dict(population.filter(half).group_by("stratum").len().iter_rows())
        allocation |= allocate(sizes, budget, minimum=PREREQUISITES_MIN_PER_STRATUM)
    sample = stratified_sample(
        population, allocation, seed=seed, set_name="prerequisites"
    )
    concepts = {row["concept_pk"]: row for row in inputs.concepts.iter_rows(named=True)}
    swapped = {"a_before_b": "b_before_a", "b_before_a": "a_before_b"}
    rows = []
    for pair in sample.iter_rows(named=True):
        flip = _unit(seed, "prerequisites-orientation", pair["item_id"]) < 0.5  # noqa: PLR2004
        a, b = (
            (pair["concept_b"], pair["concept_a"])
            if flip
            else (pair["concept_a"], pair["concept_b"])
        )
        direction = (
            swapped.get(pair["edge_direction"], pair["edge_direction"])
            if flip
            else pair["edge_direction"]
        )

        def text(concept: str) -> str:
            row = concepts[concept]
            return (
                f"{row['preferred_label']} ({row['concept_kind']})\n"
                f"{row['description'] or row['extracted_description'] or ''}"
            )

        rows.append(
            {
                "item_id": pair["item_id"],
                "stratum": pair["stratum"],
                "concept_a": a,
                "concept_b": b,
                "label_a": concepts[a]["preferred_label"],
                "label_b": concepts[b]["preferred_label"],
                "text_a": text(a),
                "text_b": text(b),
                "edge_direction": direction,
            }
        )
    return pl.DataFrame(rows)
