"""Plugin providing Kubernetes node entities.

This module implements the Kubernetes node entity.
An entity update of the same UEID will be created by the ``entityd.hostme``
plugin, with a different set of properties and attributes. The entity
in the model therefore comprises the properties and attributes from both
plugins.
"""

import kube
import requests

import entityd.pm


class NodeEntity:
    """Plugin to generate Kubernetes Node Entities."""

    def __init__(self):
        self.session = None
        self._bootid = None

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

    @property
    def bootid(self):
        """Get and store the boot ID of the executing kernel.

        :returns: Kernel's boot ID UUID string.
        """
        if self._bootid:
            return self._bootid
        with open('/proc/sys/kernel/random/boot_id', 'r') as fp:
            self._bootid = fp.read().strip()
            return self._bootid

    def getpodnodes(self, cluster):
        """Create a dictionary object of form:

            {nodename: set(node's pods), ...}.
        """
        podnodes = {}
        for pod in cluster.pods:
            podname = pod.raw.metadata.name
            nodename = pod.raw.spec.nodeName
            if nodename not in podnodes:
                podnodes[nodename] = set(podname)
            else:
                podnodes[nodename].add(podname)
        return podnodes

    def nodes(self):
        """Provide all the Kubernetes node entities."""
        with kube.Cluster() as cluster:
            podnodes = self.getpodnodes(cluster)
            try:
                for node in cluster.nodes:
                    nodename = node.meta.name
                    print("Nodename:", nodename)
                    bootid = node.raw.status['nodeInfo']['bootID']
                    yield self.create_entity(
                        nodename, bootid, podnodes[nodename])
            except requests.ConnectionError:
                return None

    def create_entity(self, nodename, bootid, pods):
        """Generator of Kubernetes Node Entities."""
        update = entityd.EntityUpdate('Kubernetes:Node')
        update.label = nodename
        update.attrs.set('bootid', bootid, {'entity:id'})
        update.attrs.set('kubernetes:kind', 'Node')
        print(update.attrs._attrs)
        print()
        return update
