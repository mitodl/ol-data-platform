import ast
from pathlib import Path

import lakehouse
import pytest
from dagster_k8s.container_context import K8sContainerContext
from dagster_k8s.job import UserDefinedDagsterK8sConfig
from lakehouse.lib.executor import (
    DBT_CLI_OPS,
    DBT_STEP_OPS,
    DBT_STEP_PRIORITY_CLASS,
    DBT_STEP_RESOURCES,
    LIGHT_STEP_RESOURCES,
    executor_for_environment,
    step_pod_executor,
)

# What a step pod inherits before the executor's config applies: the run
# launcher's priority class and the code location's resources, as deployed.
INHERITED = K8sContainerContext(
    run_k8s_config=UserDefinedDagsterK8sConfig(
        container_config={
            "resources": {
                "requests": {"cpu": "1", "memory": "2Gi"},
                "limits": {"cpu": "4", "memory": "8Gi"},
            }
        },
        pod_spec_config={"priority_class_name": "dagster-run"},
    )
)


def _step_config(op: str) -> UserDefinedDagsterK8sConfig:
    """Merge config for one step in the order K8sStepHandler does."""
    config = step_pod_executor.apply_config_mapping({}).value["config"]
    return (
        INHERITED.merge(
            K8sContainerContext(
                run_k8s_config=UserDefinedDagsterK8sConfig.from_dict(
                    config["step_k8s_config"]
                )
            )
        )
        .merge(
            K8sContainerContext(
                run_k8s_config=UserDefinedDagsterK8sConfig.from_dict(
                    config["per_step_k8s_config"].get(op, {})
                )
            )
        )
        .run_k8s_config
    )


def test_qa_pods_run_steps_in_their_own_pods():
    assert executor_for_environment("qa", in_kubernetes=True) is step_pod_executor


def test_qa_outside_kubernetes_keeps_the_default_executor():
    # docker-compose runs the local lakehouse with DAGSTER_ENVIRONMENT=qa.
    assert executor_for_environment("qa", in_kubernetes=False) is None


@pytest.mark.parametrize("dagster_env", ["dev", "ci", "production"])
def test_other_environments_keep_the_default_executor(dagster_env):
    assert executor_for_environment(dagster_env, in_kubernetes=True) is None


def test_the_executor_config_is_valid():
    assert step_pod_executor.apply_config_mapping({}).success


def test_a_light_step_shrinks_and_stays_preemptible():
    config = _step_config("iceberg_raw_layer_maintenance")

    assert config.container_config["resources"] == LIGHT_STEP_RESOURCES
    assert config.pod_spec_config["priority_class_name"] == "dagster-run"


@pytest.mark.parametrize("op", DBT_STEP_OPS)
def test_a_dbt_step_keeps_its_resources_and_leaves_the_preemptible_class(op):
    config = _step_config(op)

    assert config.container_config["resources"] == DBT_STEP_RESOURCES
    assert config.pod_spec_config["priority_class_name"] == DBT_STEP_PRIORITY_CLASS


@pytest.mark.parametrize("op", DBT_CLI_OPS)
def test_a_dbt_cli_step_gets_dbt_resources_but_stays_preemptible(op):
    config = _step_config(op)

    assert config.container_config["resources"] == DBT_STEP_RESOURCES
    assert config.pod_spec_config["priority_class_name"] == "dagster-run"


def _decorated_op_names(decorator_name: str) -> set[str]:
    """Op names of every function in the lakehouse package with this decorator.

    Read, not imported: importing evaluates @dbt_assets, which needs a manifest.
    An explicit name= is the op name; otherwise the function's name is.
    """
    names = set()
    for module in Path(lakehouse.__file__).parent.rglob("*.py"):
        for node in ast.walk(ast.parse(module.read_text())):
            if not isinstance(node, ast.FunctionDef):
                continue
            for decorator in node.decorator_list:
                call = decorator if isinstance(decorator, ast.Call) else None
                func = call.func if call else decorator
                if getattr(func, "id", None) != decorator_name:
                    continue
                explicit = [
                    keyword.value.value
                    for keyword in (call.keywords if call else [])
                    if keyword.arg == "name"
                    and isinstance(keyword.value, ast.Constant)
                    and isinstance(keyword.value.value, str)
                ]
                names.add(explicit[0] if explicit else node.name)
    return names


def test_dbt_step_ops_name_every_dbt_assets_definition():
    # A renamed or added dbt op would otherwise silently fall back to the
    # light, preemptible step config.
    assert _decorated_op_names("dbt_assets") == set(DBT_STEP_OPS)


def test_dbt_cli_ops_are_ops():
    assert set(DBT_CLI_OPS) <= _decorated_op_names("op")
