import requests

import entityd
from entityd.kubernetes import BasePlugin


class NamespaceGroup(BasePlugin):
    def __init__(self):
        super().__init__(
            entity_name='Group',
            plugin_name='entityd.kubernetes.group.NamespaceGroup'
        )

    def find_entities(self):
        """Find Kubernetes Namespace Group entities."""
        try:
            for namespace in self.cluster.namespaces:
                yield self.create_entity(namespace)
        except requests.ConnectionError:
            self.log_api_server_unreachable()
        else:
            self.logged_k8s_unreachable = False

    def create_entity(self, namespace):
        """Create an entity representing a Kubernetes Namespace Group.

        :param namespace: kube namespace item.
        :type namespace: kube._namespace.NamespaceItem
        """
        update = entityd.EntityUpdate(self._entity_name)

        update.label = namespace.meta.name
        update.attrs.set(
            'kind', "Kubernetes:Namespace", traits={'entity:id'})

        id_ueid = self.create_namespace_ueid(namespace.meta.name)
        update.attrs.set('id', str(id_ueid), traits={'entity:id'})
        update.children.add(id_ueid)

        return update
