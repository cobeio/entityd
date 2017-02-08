"""Plugin providing Kubernetes Service entities."""

import ipaddress

import kube
import requests

import entityd.kubernetes


class ServiceEntity(entityd.kubernetes.BasePlugin):
    """Plugin to generate Kubernetes Service Entities."""

    def __init__(self):
        super().__init__(
            entity_name='Kubernetes:Service',
            plugin_name='entityd.kubernetes.service.ServiceEntity'
        )

    def find_entities(self):
        """Find Kubernetes Service entities."""
        try:
            for resource in self.cluster.services:
                yield self.create_entity(resource)
        except requests.ConnectionError:
            self.log_api_server_unreachable()
        else:
            self.logged_k8s_unreachable = False

    def create_entity(self, resource):
        """Create an entity representing a Kubernetes Service.

        :param resource: kube service item.
        :type resource: kube._service.ServiceItem
        """
        pods = self.find_service_or_rc_pod_children(
            resource, self.cluster.pods.api_path)
        update = self.create_base_entity(resource, pods)
        try:
            for point in resource.loadbalancer_ingress:
                if isinstance(point, ipaddress.IPv4Address):
                    update.attrs.set('kubernetes:load-balancer-ingress',
                                     str(point), traits={'ipaddr:v4'})
                elif isinstance(point, ipaddress.IPv6Address):
                    update.attrs.set('kubernetes:load-balancer-ingress',
                                     str(point), traits={'ipaddr:v6'})
                elif isinstance(point, str):
                    update.attrs.set(
                        'kubernetes:load-balancer-ingress', point)
        except kube.StatusError:
            update.attrs.delete('kubernetes:load-balancer-ingress')
        return update
