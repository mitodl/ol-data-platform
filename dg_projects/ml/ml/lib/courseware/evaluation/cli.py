"""Build the gold-set samples from local outputs and score annotated exports.

python -m ml.lib.courseware.local gold sample gold/ \\
        --extraction out/ --extraction out_haiku/ \\
        --sources extraction_sources.parquet \\
        --identity identity/ --concept-labels concept_labels.parquet \\
        --concepts dim_concept.parquet --relations bridge_concept_relation.parquet
python -m ml.lib.courseware.local gold score gold/ --claims-export claims.json
"""

import json
from pathlib import Path
from typing import Any

import cyclopts
import polars as pl
from ml.lib.courseware.evaluation import labelstudio, samples, score
from ml.lib.courseware.evaluation.matching import MATCHING_RULE, concept_label_key
from ml.lib.courseware.evaluation.metrics import MIN_LABELS_PER_STRATUM

gold = cyclopts.App(name="gold", help=__doc__)
SETS = ("claims", "subsections", "pairs", "clusters", "prerequisites")
MANIFEST = "manifest.json"


def _write_set(  # noqa: PLR0913
    output_dir: Path,
    name: str,
    population: pl.DataFrame,
    sample: pl.DataFrame,
    tasks: list[dict[str, Any]],
    config: str,
) -> pl.DataFrame:
    population.write_parquet(output_dir / f"{name}_population.parquet")
    sample.write_parquet(output_dir / f"{name}_sample.parquet")
    (output_dir / f"{name}_tasks.json").write_text(json.dumps(tasks, indent=1))
    (output_dir / f"{name}_config.xml").write_text(config)
    sampled = sample.group_by("stratum").agg(pl.len().alias("sampled"))
    return (
        population.group_by("stratum")
        .agg(pl.len().alias("population"))
        .join(sampled, on="stratum", how="left")
        .with_columns(pl.col("sampled").fill_null(0), pl.lit(name).alias("set"))
        .select("set", "stratum", "population", "sampled")
        .sort("stratum")
    )


def _coarsened(
    name: str,
    population: pl.DataFrame,
    dropped: list[pl.DataFrame],
    min_size: int = MIN_LABELS_PER_STRATUM,
) -> pl.DataFrame:
    """Merge strata too small to estimate, recording any dropped from the set."""
    population, small = samples.coarsen_strata(
        population, min_size, samples.SCORING_GROUPS[name]
    )
    dropped.append(
        small.select(
            pl.lit(name).alias("set"),
            pl.format("(dropped) {}", "stratum").alias("stratum"),
            pl.col("population").cast(pl.UInt32),
            pl.lit(0, dtype=pl.UInt32).alias("sampled"),
        )
    )
    return population


@gold.command
def sample(  # noqa: PLR0913
    output_dir: Path,
    *,
    extraction: list[Path],
    sources: Path,
    identity: Path,
    concept_labels: Path,
    concepts: Path,
    relations: Path,
    label_map: list[Path] | None = None,
    seed: int = 20261017,
    claims: int = 400,
    subsections: int = 30,
    pairs: int = 300,
    clusters: int = 100,
    prerequisites: int = 200,
) -> None:
    """Write each set's population, sample, Label Studio tasks and config.

    :param output_dir: where the gold-set files are written
    :param extraction: a directory of claim extraction tables; repeat per model
    :param sources: int__courseware__extraction_sources as parquet
    :param identity: directory of the resolution tables
    :param concept_labels: int__courseware__concept_labels as parquet
    :param concepts: dim_concept as parquet
    :param relations: bridge_concept_relation as parquet
    :param label_map: parquet of concept_label_key -> canonical_label_key pairs
        added to the matching rule (e.g. both models' labels resolved together)
    :param seed: sampling seed; the same seed and inputs give the same samples
    :param claims: claims to verify
    :param subsections: unit scopes to annotate exhaustively
    :param pairs: label pairs to judge
    :param clusters: resolved concepts to review
    :param prerequisites: concept pairs to label, half with an inferred edge
    """
    inputs = samples.load_inputs(
        extraction, sources, identity, concept_labels, concepts, relations, label_map
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    built: list[pl.DataFrame] = []
    dropped: list[pl.DataFrame] = []

    population = _coarsened("claims", samples.claims_population(inputs), dropped)
    drawn = samples.sample_claims(inputs, population, size=claims, seed=seed)
    built.append(
        _write_set(
            output_dir,
            "claims",
            population,
            drawn,
            labelstudio.claims_tasks(drawn),
            labelstudio.CLAIMS_CONFIG,
        )
    )
    population = _coarsened(
        "subsections",
        samples.subsections_population(inputs),
        dropped,
        samples.SUBSECTIONS_MIN_PER_STRATUM,
    )
    drawn = samples.sample_subsections(inputs, population, size=subsections, seed=seed)
    built.append(
        _write_set(
            output_dir,
            "subsections",
            population,
            drawn,
            labelstudio.subsections_tasks(drawn),
            labelstudio.SUBSECTIONS_CONFIG,
        )
    )
    population = _coarsened("pairs", samples.pairs_population(inputs), dropped)
    drawn = samples.sample_pairs(inputs, population, size=pairs, seed=seed)
    built.append(
        _write_set(
            output_dir,
            "pairs",
            population,
            drawn,
            labelstudio.pairs_tasks(drawn),
            labelstudio.PAIRS_CONFIG,
        )
    )
    population = _coarsened("clusters", samples.clusters_population(inputs), dropped)
    drawn = samples.sample_clusters(inputs, population, size=clusters, seed=seed)
    groups = min(
        labelstudio.MAX_CLUSTER_GROUPS,
        max(2, int(population.select(pl.col("size").max()).item())),
    )
    built.append(
        _write_set(
            output_dir,
            "clusters",
            population,
            drawn,
            labelstudio.clusters_tasks(drawn),
            labelstudio.clusters_config(groups),
        )
    )
    population = _coarsened(
        "prerequisites", samples.prerequisites_population(inputs), dropped
    )
    drawn = samples.sample_prerequisites(
        inputs, population, size=prerequisites, seed=seed
    )
    built.append(
        _write_set(
            output_dir,
            "prerequisites",
            population,
            drawn,
            labelstudio.prerequisites_tasks(drawn),
            labelstudio.PREREQUISITES_CONFIG,
        )
    )

    strata = pl.concat(built + dropped)
    strata.write_csv(output_dir / "strata.csv")
    (output_dir / MANIFEST).write_text(
        json.dumps(
            {
                "seed": seed,
                "matching_rule": MATCHING_RULE,
                "inputs": {
                    "extraction": [str(path) for path in extraction],
                    "sources": str(sources),
                    "identity": str(identity),
                    "concept_labels": str(concept_labels),
                    "concepts": str(concepts),
                    "relations": str(relations),
                    "label_map": [str(path) for path in label_map or []],
                },
            },
            indent=1,
        )
    )
    with pl.Config(tbl_rows=-1, tbl_width_chars=120, fmt_str_lengths=60):
        print(strata)  # noqa: T201


@gold.command(name="score")
def score_gold(  # noqa: PLR0913
    sample_dir: Path,
    *,
    claims_export: Path | None = None,
    subsections_export: Path | None = None,
    pairs_export: Path | None = None,
    clusters_export: Path | None = None,
    prerequisites_export: Path | None = None,
    autorater: Path | None = None,
    gold_label_map: Path | None = None,
    alpha: float = 0.05,
) -> None:
    """Score the annotated exports against the populations the sample step wrote.

    :param sample_dir: the directory `gold sample` wrote
    :param claims_export: Label Studio JSON export of the claims project
    :param subsections_export: export of the subsections project
    :param pairs_export: export of the concept pairs project
    :param clusters_export: export of the clusters project
    :param prerequisites_export: export of the prerequisites project
    :param autorater: parquet of item_id, autorater (0 to 1) for every claim,
        replacing the cross-model prediction (e.g. an LLM judge)
    :param gold_label_map: parquet of label, model_label pairs mapping annotators'
        concept names onto model labels, made by reviewing
        unmatched_gold_concepts.csv
    :param alpha: confidence intervals target 1 - alpha coverage
    """
    manifest = json.loads((sample_dir / MANIFEST).read_text())

    def read(name: str, kind: str) -> pl.DataFrame:
        return pl.read_parquet(sample_dir / f"{name}_{kind}.parquet")

    report: dict[str, Any] = {"seed": manifest["seed"], "alpha": alpha}
    if claims_export:
        report["claims"] = score.score_claims(
            read("claims", "population"),
            labelstudio.read_export(claims_export),
            alpha=alpha,
            autorater=pl.read_parquet(autorater) if autorater else None,
        )
    if subsections_export:
        paths = manifest["inputs"]
        inputs = samples.load_inputs(
            [Path(path) for path in paths["extraction"]],
            Path(paths["sources"]),
            Path(paths["identity"]),
            Path(paths["concept_labels"]),
            Path(paths["concepts"]),
            Path(paths["relations"]),
            [Path(path) for path in paths["label_map"]],
        )
        if gold_label_map:
            inputs.matcher.add_mapping(
                (concept_label_key(label), concept_label_key(model_label))
                for label, model_label in pl.read_parquet(gold_label_map)
                .select("label", "model_label")
                .iter_rows()
            )
        model_concepts = samples.scope_concepts(inputs)
        report["subsections"], unmatched = score.score_subsections(
            read("subsections", "population"),
            read("subsections", "sample"),
            labelstudio.read_export(subsections_export),
            model_concepts,
            inputs.matcher,
            alpha=alpha,
        )
        unmatched.write_csv(sample_dir / "unmatched_gold_concepts.csv")
    for name, export, scorer in (
        ("pairs", pairs_export, score.score_pairs),
        ("clusters", clusters_export, score.score_clusters),
        ("prerequisites", prerequisites_export, score.score_prerequisites),
    ):
        if export:
            report[name] = scorer(
                read(name, "population"),
                read(name, "sample"),
                labelstudio.read_export(export),
                alpha=alpha,
            )
    (sample_dir / "scores.json").write_text(json.dumps(report, indent=1))
    print(json.dumps(report, indent=1))  # noqa: T201
