import pytest
from dagster_k8s import k8s_job_executor
from lakehouse.lib.executor import STEP_POD_ENVIRONMENTS, executor_for_environment


def test_qa_runs_steps_in_their_own_pods():
    assert executor_for_environment("qa") is k8s_job_executor


@pytest.mark.parametrize("dagster_env", ["dev", "ci", "production"])
def test_other_environments_keep_the_default_executor(dagster_env):
    assert dagster_env not in STEP_POD_ENVIRONMENTS
    assert executor_for_environment(dagster_env) is None
