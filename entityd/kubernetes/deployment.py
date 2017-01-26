"""Plugin providing Kubernetes Deployment entities."""

import kube
import requests

import entityd.kubernetes


RFC_3339_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


class DeploymentEntity(entityd.kubernetes.BasePlugin):
    """Plugin to generate Kubernetes Deployment Entities."""

    def __init__(self):
        super().__init__(
            entity_name='Kubernetes:Deployment',
            plugin_name='entityd.kubernetes.deployment.DeploymentEntity'
        )

    def find_entities(self):
        """Find Kubernetes Deployment entities."""
        try:
            replicasets = self.determine_replicaset_labels()
            for resource in self.cluster.deployments:
                yield self.create_entity(resource, replicasets)
        except requests.ConnectionError:
            self.log_api_server_unreachable()
        else:
            self.logged_k8s_unreachable = False

    def create_entity(self, resource, replicasets):
        """Create an entity representing a Kubernetes Deployment.

        :param resource: Kubernetes resource item.
        :type resource: kube._deployment.DeploymentItem
        :param dict deployments: Set of labels for each deployment in the
            cluster.
        """
        update = self.create_base_entity(resource, replicasets)
        attributes = {
            'kubernetes:observed-generation': 'observed_generation',
            'kubernetes:observed-replicas': 'observed_replicas',
            'kubernetes:updated-replicas': 'updated_replicas',
            'kubernetes:available-replicas': 'available_replicas',
            'kubernetes:unavailable-replicas': 'unavailable_replicas',
        }
        for attr in attributes:
            try:
                update.attrs.set(attr, getattr(resource, attributes[attr]))
            except kube.StatusError:
                update.attrs.delete(attr)
        spec = resource.spec()
        # todo: decide what other attributes to include
        try:
            update.attrs.set('kubernetes:replicas-desired', spec['replicas'])
        except KeyError:
            update.attrs.delete('kubernetes:replicas-desired')
        return update
