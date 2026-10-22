"""Freshness checks for dbt models, bounded by their data contract's SLA if any.

A contract in the repository's ``contracts/`` directory declares its asset's
refresh SLA. OpenMetadata stores that SLA but never evaluates it, so for a
``dbt_model`` contract the bound comes from the contract and Dagster enforces
it. Every other checked model keeps the layer-wide default.
"""

import warnings
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import timedelta
from pathlib import Path

from dagster import (
    AssetChecksDefinition,
    AssetCheckSeverity,
    AssetKey,
    AssetsDefinition,
    FreshnessPolicy,
    build_last_update_freshness_checks,
)
from dagster_dbt.asset_utils import get_asset_key_for_model
from ol_dbt_cli.lib.data_contracts import DataContract, freshness_bound
from ol_orchestrate.lib.constants import DAGSTER_ENV

from lakehouse.lib.repo_paths import find_upward

# dg_projects/lakehouse/Dockerfile copies contracts/ here.
CONTRACTS_FALLBACK = Path("/app/contracts")


def find_contracts_dir() -> Path:
    """Locate the repository's ``contracts/`` from the source tree or the image.

    Unlike a missing inventory, a missing contracts directory has no asset to
    fail on: every contracted model would quietly fall back to the default
    bound. So a deployed image without it fails to load; only dev, where a
    partial checkout is normal, gets a warning.

    :raises FileNotFoundError: outside dev, when no ``contracts/`` is found.
    """
    found = find_upward(Path(__file__), Path("contracts"), CONTRACTS_FALLBACK)
    if not found.is_dir():
        msg = (
            f"No contracts directory at {found}; every dbt model would get the "
            "default freshness bound."
        )
        if DAGSTER_ENV != "dev":
            raise FileNotFoundError(msg)
        warnings.warn(msg, stacklevel=2)
    return found


def dbt_model_key_lookup(
    dbt_assets: AssetsDefinition,
) -> Callable[[str], AssetKey | None]:
    """Map a dbt model name to its asset key in *dbt_assets*, or ``None`` if absent.

    A model the project selects out, or that only another target enables (e.g.
    the StarRocks-only models), has no asset here to check.
    """

    def lookup(model_name: str) -> AssetKey | None:
        try:
            key = get_asset_key_for_model([dbt_assets], model_name)
        except KeyError:
            return None
        return key if key in dbt_assets.keys else None

    return lookup


def contract_freshness_bounds(
    contracts: Iterable[DataContract],
    model_key: Callable[[str], AssetKey | None],
) -> dict[AssetKey, timedelta]:
    """Return the SLA freshness bound of each contracted dbt model, by asset key.

    Contracts without ``sla.refreshFrequency`` are left out, as are
    ``dbt_source`` and ``fqn`` contracts, whose assets are not dbt models.

    :param model_key: resolves a dbt model name to its asset key, ``None`` when
        the code location has no such asset.
    """
    bounds = {}
    for contract in contracts:
        bound = freshness_bound(contract)
        if bound is None:
            continue
        key = model_key(contract.entity.target)
        if key is None:
            warnings.warn(
                f"{contract.path.name} sets an SLA for dbt model "
                f"{contract.entity.target!r}, which this code location has no "
                "asset for; its freshness is not checked.",
                stacklevel=2,
            )
            continue
        bounds[key] = bound
    return bounds


def with_uncontracted_freshness_policy(
    dbt_assets: AssetsDefinition,
    keys: Iterable[AssetKey],
    contract_bounds: Mapping[AssetKey, timedelta],
    policy: FreshnessPolicy,
) -> AssetsDefinition:
    """Attach *policy* to each of *keys* that has no contract bound.

    A contracted asset's check is its one freshness signal: a policy with its
    own windows would report it stale before a longer contract bound expires.
    ``map_asset_specs`` keeps the op, the hoisted dbt-test checks and every key.
    """
    policy_keys = set(keys) - contract_bounds.keys()
    return dbt_assets.map_asset_specs(
        lambda spec: (
            spec.replace_attributes(freshness_policy=policy)
            if spec.key in policy_keys
            else spec
        )
    )


def build_freshness_checks(
    default_keys: Iterable[AssetKey],
    default_bound: timedelta,
    contract_bounds: Mapping[AssetKey, timedelta],
) -> Sequence[AssetChecksDefinition]:
    """Build one last-update freshness check per asset.

    An asset with a contract bound is checked against it, whether or not it
    is in *default_keys*; every other default key gets *default_bound*. Assets
    sharing a bound share one multi-asset check definition.
    """
    keys_by_bound: dict[timedelta, list[AssetKey]] = defaultdict(list)
    for key in default_keys:
        if key not in contract_bounds:
            keys_by_bound[default_bound].append(key)
    for key, bound in contract_bounds.items():
        keys_by_bound[bound].append(key)
    return [
        check
        for bound, keys in keys_by_bound.items()
        for check in build_last_update_freshness_checks(
            assets=keys,
            lower_bound_delta=bound,
            severity=AssetCheckSeverity.ERROR,
        )
    ]
