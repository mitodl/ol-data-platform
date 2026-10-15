"""Which executor lakehouse runs use in each environment.

The default multiprocess executor runs every step inside the run worker pod. When
that pod dies, run monitoring resumes the run in a new pod, which starts the plan
over from its first step: a full dbt build that had already finished is run again,
replacing every table it built. `k8s_job_executor` runs each step in its own pod
instead. A resumed run worker rebuilds the plan's state from the event log and
reattaches to step pods that are still running, so a dbt step outlives the run
worker that launched it.

Step pods inherit the run launcher's and the code location's container config
(service account, secrets, resources, priority class). The user-deployments
chart's default Role (rbacEnabled) is what lets the run worker's
dagster-user-code service account create those Jobs.
`enable_owner_references` stays at its default of False: an owner reference to the
run pod would let Kubernetes garbage-collect the step Jobs when the run pod is
deleted, which is the failure this exists to survive.

Rolled out per environment, QA first, like DBT_AUTOMATION_ENVIRONMENTS. Production
waits until QA runs have shown step pods come up and clean up as expected.

DAGSTER_ENV alone does not say whether there is a cluster to launch step pods in:
docker-compose runs the local lakehouse with DAGSTER_ENVIRONMENT=qa against the
default run launcher, where k8s_job_executor fails at init trying to load in-cluster
config. So the executor also requires KUBERNETES_SERVICE_HOST, which the kubelet
sets in every pod.
"""

from dagster import ExecutorDefinition
from dagster_k8s import k8s_job_executor

STEP_POD_ENVIRONMENTS: frozenset[str] = frozenset({"qa"})


def executor_for_environment(
    dagster_env: str, *, in_kubernetes: bool
) -> ExecutorDefinition | None:
    """Return the executor for ``dagster_env``.

    :param dagster_env: The DAGSTER_ENV the code location is loaded in.
    :param in_kubernetes: Whether the code location runs in a Kubernetes pod.
    :returns: ``k8s_job_executor`` in STEP_POD_ENVIRONMENTS when running in
        Kubernetes, otherwise None, which leaves Dagster's default
        multiprocess executor in place.
    :rtype: ExecutorDefinition | None
    """
    if in_kubernetes and dagster_env in STEP_POD_ENVIRONMENTS:
        return k8s_job_executor
    return None
