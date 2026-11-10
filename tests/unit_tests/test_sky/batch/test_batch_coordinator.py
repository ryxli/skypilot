"""BatchCoordinator resume after a controller restart."""
import collections
from unittest import mock

import pytest

from sky.batch import coordinator as coordinator_lib
from sky.jobs import state as managed_job_state

JOB_ID = 7


@pytest.fixture
def coordinator(monkeypatch):
    """A resuming coordinator over a 5-item dataset, with no pool to reach."""
    # Keep the coordinator from replacing pytest's SIGTERM handler.
    monkeypatch.setattr(coordinator_lib.signal, 'signal', lambda *_a: None)
    resumed = coordinator_lib.BatchCoordinator(
        dataset_path='s3://bucket/inputs.jsonl',
        output_path='s3://bucket/outputs.jsonl',
        batch_size=2,
        pool_name='pool',
        serialized_fn='',
        input_format_dict={},
        output_formats_dict=[],
        job_id=JOB_ID,
        is_resume=True)
    resumed._input_format = list(range(5))  # pylint: disable=protected-access
    for step in ('_resolve_formats', '_discover_workers',
                 '_shutdown_stale_workers', '_dispatch_all',
                 '_reduce_results_and_cleanup'):
        monkeypatch.setattr(resumed, step, mock.Mock())
    monkeypatch.setattr(managed_job_state, 'reset_dispatched_batches',
                        mock.Mock())
    monkeypatch.setattr(managed_job_state, 'save_batch_states', mock.Mock())
    return resumed


def test_resume_without_batch_records_starts_the_job_over(
        monkeypatch, coordinator):
    """A controller that died before saving the batches dispatched none."""
    monkeypatch.setattr(managed_job_state, 'get_batch_states',
                        mock.Mock(return_value=[]))

    coordinator.run()

    managed_job_state.save_batch_states.assert_called_once_with(
        JOB_ID, [[0, 1], [2, 3], [4, 4]])
    coordinator._dispatch_all.assert_called_once_with()  # pylint: disable=protected-access


def test_resume_with_batch_records_continues_from_them(monkeypatch,
                                                       coordinator):
    records = [
        {
            'batch_idx': 0,
            'start_idx': 0,
            'end_idx': 1,
            'status': 'COMPLETED',
            'retry_count': 0
        },
        {
            'batch_idx': 1,
            'start_idx': 2,
            'end_idx': 3,
            'status': 'PENDING',
            'retry_count': 1
        },
    ]
    monkeypatch.setattr(managed_job_state, 'get_batch_states',
                        mock.Mock(return_value=records))

    coordinator.run()

    managed_job_state.save_batch_states.assert_not_called()
    assert coordinator.batches == [[0, 1], [2, 3]]
    assert coordinator.pending_batches == collections.deque([1])
    coordinator._dispatch_all.assert_called_once_with()  # pylint: disable=protected-access
