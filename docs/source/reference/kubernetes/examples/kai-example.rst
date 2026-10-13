.. _kubernetes-example-kai:

Using SkyPilot with KAI Scheduler
=================================

`KAI Scheduler <https://github.com/kai-scheduler/KAI-Scheduler>`_ is a
Kubernetes scheduler with hierarchical queues, quotas and gang scheduling.
SkyPilot can submit each cluster to a KAI queue as a gang, so all of its pods
are scheduled together.

Prerequisites
-------------

* KAI Scheduler. Tested with v0.17.
* A KAI ``Queue`` to submit to.
* Permission to get, create, patch and delete ``podgroups.scheduling.run.ai``
  in the namespace SkyPilot launches into, and to get
  ``queues.scheduling.run.ai``.

Configuration
-------------

Set the queue in ``~/.sky/config.yaml``, per context or for all contexts:

.. code-block:: yaml

    kubernetes:
      context_configs:
        my-context:
          kai:
            queue: team-a
            preemptibility: preemptible  # Optional.

A task can set it under ``config.kubernetes.kai``, unless ``~/.sky/config.yaml``
sets it for the task's context, which takes precedence.

If your KAI installation runs scheduling shards, set the shard's label with
:ref:`custom_metadata <config-yaml-kubernetes-custom-metadata>`. A shard only
schedules ``PodGroup`` objects that carry its label.

Behavior
--------

* SkyPilot creates a ``PodGroup`` for the cluster before its pods, with
  ``minMember`` set to the number of nodes, and deletes it on ``sky down``.
* While KAI keeps the cluster queued, the launch request waits on the API
  server without holding a worker, and resumes once KAI schedules it.
* The ``PodGroup`` has the same labels as the pods, so labels set with
  :ref:`custom_metadata <config-yaml-kubernetes-custom-metadata>` apply to both.
* KAI orders and preempts the gang by the ``PodGroup``'s ``PriorityClass``,
  which SkyPilot takes from ``resources.priority_class``. The pods get the same
  ``PriorityClass``.
* If the cluster requests more GPUs than the limit of its queue or a parent
  queue, or the queue does not exist, the launch fails. Other resources over a
  limit keep the launch queued.
* Relaunching onto a running cluster cannot change its queue, priority class
  or preemptibility. Run ``sky down`` first.
* When KAI preempts a gang, it deletes the cluster's pods. ``sky status
  --refresh`` then removes the cluster, and a :ref:`managed job <managed-jobs>`
  recovers onto a new one.
* Cancelling a queued launch leaves the cluster's pods in the queue. Run
  ``sky down`` to remove them.
* A queued launch prints KAI's reason for keeping it queued.
  ``kubectl describe podgroup <cluster>`` shows the full scheduling conditions.

Limitations
-----------

* KAI and :ref:`Kueue <kubernetes-example-kueue>` cannot be used for the same
  cluster.
* High availability controllers are not supported.
