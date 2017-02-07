"""Plugin providing Kubernetes Daemon Set entities."""

import kube
import requests

import entityd.kubernetes


class DaemonSetEntity(entityd.kubernetes.BasePlugin):
    """Plugin to generate Kubernetes Daemon Set Entities."""

    def __init__(self):
        super().__init__(
            entity_name='Kubernetes:DaemonSet',
            plugin_name='entityd.kubernetes.daemonset.DaemonSetEntity'
        )

    def find_entities(self):
        """Find Kubernetes Daemon Set entities."""
        try:
            pods = self.determine_pods_labels()
            for resource in self.cluster.daemonsets:
                yield self.create_entity(resource, pods)
        except requests.ConnectionError:
            self.log_api_server_unreachable()
        else:
            self.logged_k8s_unreachable = False

    def create_entity(self, resource, pods):
        """Create an entity representing a Kubernetes Daemon Set.

        :param resource: Kubernetes resource item.
        :type resource: kube._daemonset.DaemonSetItem
        :param dict pods: Set of labels for each pod in the cluster.
        """
        update = self.create_base_entity(resource, pods)
        attributes = {
            'kubernetes:current-number-scheduled': 'current_number_scheduled',
            'kubernetes:number-misscheduled': 'number_misscheduled',
            'kubernetes:desired-number-scheduled': 'desired_number_scheduled',
            'kubernetes:number-ready': 'number_ready',
        }
        for attr in attributes:
            try:
                update.attrs.set(attr, getattr(resource, attributes[attr]))
            except kube.StatusError:
                update.attrs.delete(attr)
        return update
