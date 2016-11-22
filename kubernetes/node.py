"""Plugin providing Kubernetes node entities.

This module implements the Kubernetes node entity.
An entity update of the same UEID will be created by the ``entityd.hostme``
plugin, with a different set of properties and attributes. The entity
in the model therefore comprises the properties and attributes from both
plugins.
"""

import collections
import kube
import requests

import logbook

import entityd.pm


log = logbook.Logger(__name__)
_LOGGED_K8S_UNREACHABLE = False
Poddata = collections.namedtuple('Poddata', ['name', 'namespace'])


class NodeEntity:
    """Plugin to generate Kubernetes Node Entities."""

    def __init__(self):
        self.session = None

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Register the Node Entity."""
        config.addentity('Kubernetes:Node', 'kubernetes.node.NodeEntity')

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        """Store the session for later usage."""
        self.session = session

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs, include_ondemand=False):  # pylint: disable=unused-argument
        """Return an iterator of Kubernetes "Node" Entities."""
        if name == 'Kubernetes:Node':
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.nodes()

    @staticmethod
    def get_pod_ueid(podname, namespace):
        """Provide a pod's ueid.

        :param str podname: Pod's name.
        :param str namespace: Pod's namespace.

        :returns: A :class:`cobe.UEID` for the pod.
        """
        update = entityd.EntityUpdate('Pod')
        update.attrs.set('meta:name', podname, traits={'entity:id'})
        update.attrs.set('meta:namespace', namespace, traits={'entity:id'})
        return update.ueid

    def getnodespods(self, cluster):
        """Determine the pods that exist in each namespace.

        Note that pods not yet assigned to a node are skipped by handling the
        KeyError that occurs under such circumstances.

        :returns: A dict of form {nodename: set(node's pods), ...}.
        """
        nodepods = {}
        for pod in cluster.pods:
            podname = pod.raw['metadata']['name']
            podnamespace = pod.raw['metadata']['namespace']
            try:
                nodename = pod.raw['spec']['nodeName']
            except KeyError:
                continue
            podentry = Poddata(name=podname, namespace=podnamespace)
            if nodename not in nodepods:
                nodepods[nodename] = {podentry}
            else:
                nodepods[nodename].add(podentry)
        return nodepods

    def nodes(self):
        """Provide all the Kubernetes node entities."""
        global _LOGGED_K8S_UNREACHABLE  # pylint: disable=global-statement
        with kube.Cluster() as cluster:
            try:
                nodepods = self.getnodespods(cluster)
                for node in cluster.nodes:
                    yield self.create_entity(node, nodepods)
            except requests.ConnectionError:
                if not _LOGGED_K8S_UNREACHABLE:
                    log.info('Kubernetes API server unreachable')
                    _LOGGED_K8S_UNREACHABLE = True
            else:
                _LOGGED_K8S_UNREACHABLE = False

    def create_entity(self, node, nodepods):
        """Generator of Kubernetes Node Entities."""
        nodename = node.meta.name
        update = entityd.EntityUpdate('Kubernetes:Node')
        update.label = nodename
        update.attrs.set(
            'bootid', node.raw['status']['nodeInfo']['bootID'], {'entity:id'})
        update.attrs.set('kubernetes:kind', 'Node')
        for Poddata in nodepods.get(nodename, []):
            update.children.add(self.get_pod_ueid(Poddata.name,
                                                  Poddata.namespace))
        return update
