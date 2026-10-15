import pytest
from dagster_k8s import k8s_job_executor
from lakehouse.lib.executor import executor_for_environment


def test_qa_pods_run_steps_in_their_own_pods():
    assert executor_for_environment("qa", in_kubernetes=True) is k8s_job_executor


def test_qa_outside_kubernetes_keeps_the_default_executor():
    # docker-compose runs the local lakehouse with DAGSTER_ENVIRONMENT=qa.
    assert executor_for_environment("qa", in_kubernetes=False) is None


@pytest.mark.parametrize("dagster_env", ["dev", "ci", "production"])
def test_other_environments_keep_the_default_executor(dagster_env):
    assert executor_for_environment(dagster_env, in_kubernetes=True) is None
