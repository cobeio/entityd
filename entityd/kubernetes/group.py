"""Plugin providing namespace groups entities"""

import logbook
import requests

import entityd

log = logbook.Logger(__name__)


class NamespaceGroup:
    """Entity for Kubernetes Namespace Groups"""

    name = "GroupK"
    kind = "Kubernetes:Namespace"
    _cluster_ueid = None

    def __init__(self):
        self.cluster = None
        self.logged_k8s_unreachable = False
        self.session = None

    def find_entities(self):
        """Find Kubernetes Namespace Group entities."""
        try:
            for namespace in self.cluster.namespaces:
                yield self.generate_updates(namespace)
        except requests.ConnectionError:
            self.log_api_server_unreachable()
        else:
            self.logged_k8s_unreachable = False

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Register the namespace group entity."""
        config.addentity(self.name,
                         'entityd.kubernetes.group.NamespaceGroup')

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs=None, include_ondemand=False):  # pylint: disable=unused-argument
        """Find the namespace group group entities"""
        if name == self.name:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.find_entities()

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        """Store the session for later usage."""
        self.session = session
        self.cluster = session.svc.kube_cluster

    @classmethod
    def get_cluster_ueid(cls, session):
        """Property to get the Kubernetes Cluster UEID.

        :raises LookupError: If a Cluster UEID cannot be found.

        :returns: A :class:`cobe.UEID` for the Cluster.
        """
        if not cls._cluster_ueid:
            results = session.pluginmanager.hooks.entityd_find_entity(
                name='Kubernetes:Cluster', attrs=None)
            if results:
                for cluster_entity in results[0]:
                    cls._cluster_ueid = cluster_entity.ueid
        if not cls._cluster_ueid:
            raise LookupError('Could not find the Cluster UEID')
        return cls._cluster_ueid

    @classmethod
    def create_namespace_ueid(cls, namespace, session):
        """Create the ueid for a namespace.

        :param str namespace: Name of namespace.

        :returns: A :class:`cobe.UEID` for the namespace.
        """
        update = entityd.EntityUpdate('Kubernetes:Namespace')
        update.attrs.set('kubernetes:meta:name',
                         namespace, traits={'entity:id'})
        update.attrs.set('cluster', str(cls.get_cluster_ueid(session)),
                         traits={'entity:id', 'entity:ueid'})
        return update.ueid

    @classmethod
    def get_ueid(cls, namespace_name, session):
        """Get the ueid for a namespace group"""
        entity = entityd.EntityUpdate(cls.name)
        entity.attrs.set('kind', cls.kind, traits={'entity:id'})
        id_ueid = cls.create_namespace_ueid(namespace_name, session)
        entity.attrs.set('id', str(id_ueid), traits={'entity:id'})

        return entity.ueid

    def log_api_server_unreachable(self):
        """Log once that the Kubernetes API server is unreachable."""
        if not self.logged_k8s_unreachable:
            log.info('Kubernetes API server unreachable')
            self.logged_k8s_unreachable = True

    def generate_updates(self, namespace):
        """Create an entity representing a Kubernetes Namespace Group.

        :param namespace: kube namespace item.
        :type namespace: kube._namespace.NamespaceItem
        """
        id_ueid = self.create_namespace_ueid(namespace.meta.name, self.session)

        update = entityd.EntityUpdate(self.name)
        update.label = namespace.meta.name
        update.attrs.set('kind', self.kind, traits={'entity:id'})
        update.attrs.set('id', str(id_ueid), traits={'entity:id'})
        update.children.add(id_ueid)

        return update
