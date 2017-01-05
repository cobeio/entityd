"""Plugin providing Kubernetes Replica Set entities."""

import kube
import requests

import entityd.kubernetes


RFC_3339_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


class DeploymentEntity(entityd.kubernetes.BasePlugin):
    """Plugin to generate Kubernetes Replica Set Entities."""

    def __init__(self):
        super().__init__()
        self.entity_name = 'Kubernetes:Deployment'
        self.plugin_name = 'entityd.kubernetes.deployment.DeploymentEntity'

    def find_entities(self):
        """Find Kubernetes Replica Set entities."""
        try:
            replicasets = self.determine_replicaset_labels()
            for resource in self.cluster.deployments:
                yield self.create_entity(resource, replicasets)
        except requests.ConnectionError:
            self.log_api_server_unreachable()
        else:
            self._logged_k8s_unreachable = False

    def create_base_entity(self, resource, replicasets):
        """Creator of the base entity for certain Kubernetes resources.

        This provides the base entity for Replica Sets and Services.

        :param resource: Kubernetes resource item.
        :type resource: kube._replicaset.ReplicaSetItem
        :param dict replicasets: Set of labels for each replicaset in the
            cluster.
        """
        meta = resource.meta
        kind = str(resource.kind).replace('Kind.', '')
        update = entityd.EntityUpdate('Kubernetes:{}'.format(kind))
        update.label = meta.name
        update.attrs.set('kubernetes:kind', kind)
        update.attrs.set('kubernetes:meta:name',
                         meta.name, traits={'entity:id'})
        update.attrs.set('kubernetes:meta:namespace',
                         meta.namespace, traits={'entity:id'})
        update.attrs.set('cluster', str(self.cluster_ueid),
                         traits={'entity:id', 'entity:ueid'})
        update.attrs.set('kubernetes:meta:version', meta.version)
        update.attrs.set('kubernetes:meta:created',
                         meta.created.strftime(RFC_3339_FORMAT),
                         traits={'chrono:rfc3339'})
        update.attrs.set('kubernetes:meta:link', meta.link, traits={'uri'})
        update.attrs.set('kubernetes:meta:uid', meta.uid)
        labels = set(meta.labels.items())
        for replicaset in replicasets:
            if labels.issubset(replicasets[replicaset]):
                update.children.add(replicaset)
        update.parents.add(self.create_namespace_ueid(meta.namespace))
        return update

    def create_entity(self, resource, replicasets):
        """Create an entity representing a Kubernetes Replica Set.

        :param resource: Kubernetes resource item.
        :type resource: kube._deployment.DeploymentItem
        :param dict replicasets: Set of labels for each replicaset in the
            cluster.
        """
        update = self.create_base_entity(resource, replicasets)
        attributes = {
            'kubernetes:replicas': 'replicas',
            'kubernetes:observed-generation': 'observed_generation',
            'kubernetes:updated-replicas': 'updated_replicas',
            'kubernetes:available-replicas': 'available_replicas',
            'kubernetes:unavailable-replicas': 'unavailable_replicas',
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
        try:
            update.attrs.set('kubernetes:paused', spec['paused'])
        except KeyError:
            pass
        return update
