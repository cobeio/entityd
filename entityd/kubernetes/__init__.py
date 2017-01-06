"""This subpackage contains the Kubernetes modules of entityd."""

from abc import ABCMeta, abstractmethod

import kube
import logbook

import entityd.entityupdate


RFC_3339_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
log = logbook.Logger(__name__)


class BasePlugin(metaclass=ABCMeta):
    """Base plugin class supporting creation of Kubernetes entities."""

    def __init__(self, entity_name, plugin_name):
        self._entity_name = entity_name
        self._plugin_name = plugin_name
        self.session = None
        self.cluster = kube.Cluster()
        self._cluster_ueid = None
        self.logged_k8s_unreachable = None

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Register the Replica Set Entity."""
        config.addentity(self._entity_name, self._plugin_name)

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        """Store the session for later usage."""
        self.session = session

    @entityd.pm.hookimpl
    def entityd_sessionfinish(self):
        """Safely terminate the plugin."""
        self.cluster.close()

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs, include_ondemand=False):  # pylint: disable=unused-argument
        """Return an iterator of Kubernetes Replica Set entities."""
        if name == self._entity_name:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.find_entities()

    @abstractmethod
    def find_entities(self):
        """Find and yield Kubernetes entities."""

    @property
    def cluster_ueid(self):
        """Property to get the Kubernetes Cluster UEID.

        :raises LookupError: If a Cluster UEID cannot be found.

        :returns: A :class:`cobe.UEID` for the Cluster.
        """
        if not self._cluster_ueid:
            results = self.session.pluginmanager.hooks.entityd_find_entity(
                name='Kubernetes:Cluster', attrs=None)
            if results:
                for cluster_entity in results[0]:
                    self._cluster_ueid = cluster_entity.ueid
        if not self._cluster_ueid:
            raise LookupError('Could not find the Cluster UEID')
        return self._cluster_ueid

    def create_pod_ueid(self, podname, namespace):
        """Create the ueid for a pod.

        :param str podname: Pod's name.
        :param str namespace: Pod's namespace.

        :returns: A :class:`cobe.UEID` for the pod.
        """
        update = entityd.EntityUpdate('Kubernetes:Pod')
        update.attrs.set('kubernetes:meta:name', podname, traits={'entity:id'})
        update.attrs.set(
            'kubernetes:meta:namespace', namespace, traits={'entity:id'})
        update.attrs.set(
            'cluster', str(self.cluster_ueid), traits={'entity:id'})
        return update.ueid

    def create_namespace_ueid(self, namespace):
        """Create the ueid for a namespace.

        :param str namespace: Name of namespace.

        :returns: A :class:`cobe.UEID` for the namespace.
        """
        update = entityd.EntityUpdate('Kubernetes:Namespace')
        update.attrs.set('kubernetes:meta:name',
                         namespace, traits={'entity:id'})
        update.attrs.set('cluster', str(self.cluster_ueid),
                         traits={'entity:id', 'entity:ueid'})
        return update.ueid

    def determine_pods_labels(self):
        """Determine the set of labels for each pod in the cluster.

        :returns: A dict of form:
            {pod :class:`cobe.UEID`: {(label_key1, label_value1), ...}, ...}
        """
        pods = {}
        for pod in self.cluster.pods:
            pod_ueid = self.create_pod_ueid(pod.meta.name, pod.meta.namespace)
            pods[pod_ueid] = set(pod.meta.labels.items())
        return pods

    def log_api_server_unreachable(self):
        """Log once that the Kubernetes API server is unreachable."""
        if not self.logged_k8s_unreachable:
            log.info('Kubernetes API server unreachable')
            self.logged_k8s_unreachable = True

    def create_base_entity(self, resource, pods):
        """Creator of the base entity for certain Kubernetes resources.

        This provides the base entity for Replica Sets and Services.

        :param resource: Kubernetes resource item.
        :type resource: kube.ReplicaSetItem | kube.ServiceItem
        :param dict pods: Set of labels for each pod in the cluster.
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
        for pod in pods:
            if labels.issubset(pods[pod]):
                update.children.add(pod)
        update.parents.add(self.create_namespace_ueid(meta.namespace))
        return update
