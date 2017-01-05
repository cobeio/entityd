"""Plugin providing Kubernetes Replication Controller entities."""

import kube
import requests

import entityd.kubernetes


class ReplicationControllerEntity(entityd.kubernetes.BasePlugin):
    """Plugin to generate Kubernetes Replication Controller Entities."""

    def __init__(self):
        super().__init__()
        self.entity_name = 'Kubernetes:ReplicationController'
        self.plugin_name = 'entityd.kubernetes.' \
                           'replicationcontroller.ReplicationControllerEntity'

    def find_entities(self):
        """Find Kubernetes Replication Controller entities."""
        try:
            pods = self.determine_pods_labels()
            for resource in self.cluster.replicationcontrollers:
                yield self.create_entity(resource, pods)
        except requests.ConnectionError:
            self.log_api_server_unreachable()
        else:
            self._logged_k8s_unreachable = False

    def create_entity(self, resource, pods):
        """Create an entity representing a Kubernetes Replication Controller.

        :param resource: Kubernetes resource item.
        :type resource: kube._replicaset.ReplicaSetItem
        :param dict pods: Set of labels for each pod in the cluster.
        """
        update = self.create_base_entity(resource, pods)
        attributes = {
            'kubernetes:observed-replicas': 'observed_replicas',
            'kubernetes:observed-generation': 'observed_generation',
            'kubernetes:fully-labeled-replicas': 'fully_labeled_replicas',
        }
        for attr in attributes:
            try:
                update.attrs.set(attr, getattr(resource, attributes[attr]))
            except kube.StatusError:
                pass
        spec = resource.spec()
        try:
            update.attrs.set('kubernetes:replicas-desired', spec['replicas'])
        except KeyError:
            pass
        return update
