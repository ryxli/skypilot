"""Exercise generated Ray run failure policies without running Ray."""
import ast
import sys
from types import SimpleNamespace
from typing import List
from unittest import mock

import pytest

from sky import clouds
from sky import task as task_lib
from sky.backends import cloud_vm_ray_backend
from sky.backends import task_codegen

_HEAD_POLICY = task_codegen.RunFailurePolicy(cancel_on_failure=False,
                                             status_from='head')


class _RayError(Exception):
    pass


def _build(codegen):
    codegen.add_prologue(job_id=1)
    codegen.add_setup(2, {'CPU': 2.0}, ['10.0.0.1', '10.0.0.2'], {}, '/logs')
    codegen.add_task(2, 'echo run', 'test', {'CPU': 2.0}, '/logs')
    codegen.add_epilogue()
    return codegen.build()


def _run_wait(code, responses, ready, head_future_index=0):
    tree = ast.parse(code)
    order = iter(ready)

    def wait(futures):
        task = next(order)
        assert task in futures
        return [task], [future for future in futures if future != task]

    def get(task):
        result = responses[task]
        if isinstance(result, _RayError):
            raise result
        return {'return_code': result, 'pid': 123}

    ray = SimpleNamespace(ObjectRef=str,
                          wait=wait,
                          get=get,
                          cancel=mock.Mock(),
                          exceptions=SimpleNamespace(RayError=_RayError))
    ray_util = SimpleNamespace(remove_placement_group=mock.Mock())
    pg = object()
    namespace = dict(ray=ray,
                     ray_util=ray_util,
                     List=List,
                     sys=sys,
                     futures=list(responses),
                     pg=pg,
                     head_future_index=head_future_index)
    definitions = [
        node for node in tree.body
        if (isinstance(node, ast.FunctionDef) and node.name == 'get_or_fail') or
        (isinstance(node, ast.Assign) and
         isinstance(node.targets[0], ast.Name) and node.targets[0].id in
         {'CANCELLED_RETURN_CODE', 'NODE_LOST_RETURN_CODE'})
    ]
    exec(
        compile(ast.Module(body=[*definitions, tree.body[-2]], type_ignores=[]),
                '<driver wait>', 'exec'), namespace)
    ray_util.remove_placement_group.assert_called_once_with(pg)
    return namespace, ray.cancel


def test_worker_ray_error_does_not_cancel_head():
    code = _build(task_codegen.RayCodeGen(failure_policy=_HEAD_POLICY))
    namespace, cancel = _run_wait(code, {
        'head': 0,
        'worker': _RayError()
    }, ['worker', 'head'])
    assert namespace['returncodes'] == [0, 250]
    cancel.assert_not_called()


def test_head_exit_cancels_unready_worker():
    code = _build(task_codegen.RayCodeGen(failure_policy=_HEAD_POLICY))
    namespace, cancel = _run_wait(code, {'head': 0, 'worker': None}, ['head'])
    assert namespace['returncodes'] == [0, 137]
    cancel.assert_called_once_with('worker', force=True)


def test_default_policy_worker_failure_cancels_head():
    code = _build(task_codegen.RayCodeGen())
    namespace, cancel = _run_wait(code, {'head': None, 'worker': 1}, ['worker'])
    assert namespace['returncodes'] == [137, 1]
    cancel.assert_called_once_with('head', force=True)


def test_explicit_default_policy_matches_implicit_default():
    assert _build(
        task_codegen.RayCodeGen(
            failure_policy=task_codegen.RunFailurePolicy())) == _build(
                task_codegen.RayCodeGen())


@pytest.mark.parametrize('head_return_code', [0, 137])
def test_epilogue_uses_head_return_code(head_return_code):
    code = _build(task_codegen.RayCodeGen(failure_policy=_HEAD_POLICY))
    namespace, cancel = _run_wait(code, {
        'worker': None,
        'head': head_return_code
    }, ['head'],
                                  head_future_index=1)
    assert namespace['returncodes'] == [137, head_return_code]
    cancel.assert_called_once_with('worker', force=True)
    namespace.update(job_lib=mock.Mock(),
                     time=mock.Mock(),
                     constants=SimpleNamespace(SKYLET_VERSION=28))
    epilogue = compile(
        ast.Module(body=ast.parse(code).body[-1:], type_ignores=[]),
        '<driver epilogue>', 'exec')
    job_lib = namespace['job_lib']
    if head_return_code == 0:
        exec(epilogue, namespace)
        job_lib.set_status.assert_called_once_with(1,
                                                   job_lib.JobStatus.SUCCEEDED)
        job_lib.set_exit_codes.assert_not_called()
    else:
        with pytest.raises(SystemExit) as exc_info:
            exec(epilogue, namespace)
        assert exc_info.value.code == 1
        job_lib.set_status.assert_called_once_with(1, job_lib.JobStatus.FAILED)
        job_lib.set_exit_codes.assert_called_once_with(1, [137, 137])


def test_run_without_a_head_task_fails_on_any_node():
    code = _build(task_codegen.RayCodeGen(failure_policy=_HEAD_POLICY))
    namespace, cancel = _run_wait(code, {
        'worker1': 0,
        'worker2': 3
    }, ['worker1', 'worker2'],
                                  head_future_index=None)
    assert namespace['returncodes'] == [0, 3]
    cancel.assert_not_called()
    namespace.update(job_lib=mock.Mock(),
                     time=mock.Mock(),
                     constants=SimpleNamespace(SKYLET_VERSION=28))
    epilogue = compile(
        ast.Module(body=ast.parse(code).body[-1:], type_ignores=[]),
        '<driver epilogue>', 'exec')
    with pytest.raises(SystemExit):
        exec(epilogue, namespace)
    job_lib = namespace['job_lib']
    job_lib.set_status.assert_called_once_with(1, job_lib.JobStatus.FAILED)


def test_task_yaml_policy_reaches_multi_node_driver():
    task = task_lib.Task.from_yaml_str('''
num_nodes: 2
run: echo run
config:
  run_failure_policy:
    cancel_on_failure: false
    max_retries: 0
    status_from: head
''')
    handle = SimpleNamespace(launched_resources=SimpleNamespace(
        cloud=clouds.Kubernetes(), accelerators=None),
                             internal_ips=lambda: ['10.0.0.1', '10.0.0.2'],
                             num_ips_per_node=1,
                             cached_cluster_info=None)
    backend = cloud_vm_ray_backend.CloudVmRayBackend.__new__(
        cloud_vm_ray_backend.CloudVmRayBackend)
    backend._setup_cmd = None
    backend._get_task_env_vars = mock.Mock(return_value={})
    backend._exec_code_on_head = mock.Mock()
    backend._execute_task_n_nodes(handle, task, 1, '/logs')
    code = backend._exec_code_on_head.call_args.args[1]
    assert code.count('max_retries=0') == 2
    assert 'head_future_index = len(futures)' in code
    namespace, cancel = _run_wait(code, {
        'head': 0,
        'worker': _RayError()
    }, ['worker', 'head'])
    assert namespace['returncodes'] == [0, 250]
    cancel.assert_not_called()
