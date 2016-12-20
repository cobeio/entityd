"""Plugin providing Kubernetes Service entities."""

import kube
import requests

import entityd.kubernetes


class ServiceEntity(entityd.kubernetes.BasePlugin):
    """Plugin to generate Kubernetes Service Entities."""

    def __init__(self):
        super().__init__()
        self.entity_name = 'Kubernetes:Service'
        self.plugin_name = 'entityd.kubernetes.service.ServiceEntity'

    def find_entities(self):
        """Find Kubernetes Service entities."""
        try:
            pods = self.determine_pods_labels()
            for resource in self.cluster.services:
                yield self.create_entity(resource, pods)
        except requests.ConnectionError:
            self.log_api_server_unreachable()
        else:
            self._logged_k8s_unreachable = False

    def create_entity(self, resource, pods):
        """Create an entity representing a Kubernetes Service.

        :param resource: Kubernetes resource item.
        :type resource: kube._service.ServiceItem
        :param dict pods: Set of labels for each pod in the cluster.
        """
        update = self.create_base_entity(resource, pods)
        try:
            update.attrs.set('kubernetes:load', resource.loadbalancer_ingress)
        except kube.StatusError:
            pass
        return update
