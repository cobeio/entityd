"""Plugin providing Kubernetes Deployment entities."""

import kube
import requests

import entityd.kubernetes


class DeploymentEntity(entityd.kubernetes.BasePlugin):
    """Plugin to generate Kubernetes Deployment entities."""

    def __init__(self):
        super().__init__(
            entity_name='Kubernetes:Deployment',
            plugin_name='entityd.kubernetes.deployment.DeploymentEntity'
        )

    def find_entities(self):
        """Find Kubernetes Deployment entities."""
        try:
            for resource in self.cluster.deployments:
                yield self.create_entity(resource)
        except requests.ConnectionError:
            self.log_api_server_unreachable()
        else:
            self.logged_k8s_unreachable = False

    def create_entity(self, resource):
        """Create an entity representing a Kubernetes Deployment.

        :param resource: kube deployment item.
        :type resource: kube._deployment.DeploymentItem
        """
        replicasets = self.find_deployment_rs_children(
            resource, self.cluster.replicasets.api_path)
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
        try:
            update.attrs.set('kubernetes:replicas-desired', spec['replicas'])
        except KeyError:
            update.attrs.delete('kubernetes:replicas-desired')
        return update
