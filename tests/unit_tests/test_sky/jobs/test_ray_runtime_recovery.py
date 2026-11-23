"""Test opt-in recovery observation on Ray-backed handles."""
from types import SimpleNamespace
from unittest import mock

from sky import clouds
from sky.jobs import runtime
from sky.skylet import job_lib


class _Owner:

    def __init__(self, observation, *, owns=True):
        self.owns = mock.Mock(return_value=owns)
        self.get_recovery_status = mock.Mock(return_value=observation)


class _ObservingOwner(_Owner):
    observes_ray_clusters = True


def test_only_opted_in_ray_owner_observes_recovery(monkeypatch):
    monkeypatch.setattr(runtime, '_runtimes', [])
    assert not runtime.observes_recovery()
    handle = SimpleNamespace(
        launched_resources=SimpleNamespace(cloud=clouds.Kubernetes()),
        provision_runtime_metadata=SimpleNamespace(has_ray=True))
    observation = runtime.RuntimeObservation('cluster',
                                             0,
                                             job_lib.JobStatus.RUNNING,
                                             nodes=['head'])
    legacy = _Owner(observation)
    non_owner = _ObservingOwner(observation, owns=False)
    opted_in = _ObservingOwner(observation)
    runtime.register(legacy)
    runtime.register(non_owner)
    runtime.register(opted_in)

    assert runtime.observes_recovery()
    kwargs = dict(job_id=1, task_id=0, task=object(), previous=None)
    assert runtime.get_recovery_status(handle, 'cluster', **
                                       kwargs) is observation
    opted_in.get_recovery_status.assert_called_once_with(
        handle, 'cluster', **kwargs)
    non_owner.owns.assert_called_once_with(handle)
    non_owner.get_recovery_status.assert_not_called()
    legacy.owns.assert_not_called()
    legacy.get_recovery_status.assert_not_called()
    assert runtime.get_exit_codes(handle) is None
