"""Plugin providing Kubernetes Replica Set entities."""

import kube
import requests

import entityd.kubernetes


class ReplicaSetEntity(entityd.kubernetes.BasePlugin):
    """Plugin to generate Kubernetes Replica Set Entities."""

    def __init__(self):
        super().__init__(
            entity_name='Kubernetes:ReplicaSet',
            plugin_name='entityd.kubernetes.replicaset.ReplicaSetEntity'
        )

    def find_entities(self):
        """Find Kubernetes Replica Set entities."""
        try:
            pods = self.determine_pods_labels()
            for resource in self.cluster.replicasets:
                yield self.create_entity(resource, pods)
        except requests.ConnectionError:
            self.log_api_server_unreachable()
        else:
            self.logged_k8s_unreachable = False

    def create_entity(self, resource, pods):
        """Create an entity representing a Kubernetes Replica Set.

        :param resource: Kubernetes resource item.
        :type resource: kube._replicaset.ReplicaSetItem
        :param dict pods: Set of labels for each pod in the cluster.
        """
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
