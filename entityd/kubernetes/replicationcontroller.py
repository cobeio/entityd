"""Plugin providing Kubernetes Replication Controller entities."""

import kube
import requests

import entityd.kubernetes


class ReplicationControllerEntity(entityd.kubernetes.BasePlugin):
    """Plugin to generate Kubernetes Replication Controller Entities."""

    def __init__(self):
        super().__init__(
            entity_name='Kubernetes:ReplicationController',
            plugin_name='entityd.kubernetes.'
                        'replicationcontroller.ReplicationControllerEntity'
        )

    def find_entities(self):
        """Find Kubernetes Replication Controller entities."""
        try:
            for resource in self.cluster.replicationcontrollers:
                yield self.create_entity(resource)
        except requests.ConnectionError:
            self.log_api_server_unreachable()
        else:
            self.logged_k8s_unreachable = False

    def create_entity(self, resource):
        """Create an entity representing a Kubernetes Replication Controller.

        :param resource: kube replication controller item.
        :type resource: kube._replicaset.ReplicationControllerItem
        """
        pods = self.find_service_or_rc_pod_children(
            resource, self.cluster.pods.api_path)
        update = self.create_base_entity(resource, pods)
        attributes = {
            'kubernetes:observed-replicas': 'observed_replicas',
            'kubernetes:observed-generation': 'observed_generation',
            'kubernetes:fully-labeled-replicas': 'fully_labeled_replicas',
            'kubernetes:ready-replicas': 'ready_replicas',
            'kubernetes:available-replicas': 'available_replicas',
        }
        for attr in attributes:
            try:
                update.attrs.set(attr, getattr(resource, attributes[attr]))
            except kube.StatusError:
                update.attrs.delete(attr)
        spec = resource.spec()
        try:
            update.attrs.set('kubernetes:replicas-desired', spec['replicas'])
        except KeyError:
            update.attrs.delete('kubernetes:replicas-desired')
        return update
