"""Plugin providing Kubernetes Cluster entity."""

import re

import kube
import logbook
import requests

import entityd.pm


log = logbook.Logger(__name__)


class ClusterEntity:
    """Plugin to generate Kubernetes Cluster Entities."""

    def __init__(self):
        self.session = None
        self._cluster = None
        self._logged_k8s_unreachable = False
        self._address = None
        self._project = None

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Register the Cluster Entity."""
        config.addentity('Kubernetes:Cluster',
                         'entityd.kubernetes.cluster.ClusterEntity')

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        """Store the session for later usage."""
        self.session = session
        self._cluster = kube.Cluster()

    @entityd.pm.hookimpl
    def entityd_sessionfinish(self):
        """Safely terminate the plugin."""
        self._cluster.close()

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs, include_ondemand=False):  # pylint: disable=unused-argument
        """Return an iterator of Kubernetes "Cluster" Entities."""
        if name == 'Kubernetes:Cluster':
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.find_cluster_entity()

    @property
    def address(self):
        """Provide the Kubernetes Cluster address."""
        if self._address:
            return self._address
        result = self._cluster.proxy.get(
            'api/v1/namespaces/default/endpoints/kubernetes')
        try:
            subsets = result['subsets'][0]
            address = subsets['addresses'][0]['ip']
            port = subsets['ports'][0]['port']
            name = subsets['ports'][0]['name']
            self._address = (
                '{}://{}:{}/').format(name, address, port)
        except (KeyError, TypeError, IndexError) as err:
            log.error('Kubernetes endpoint data '
                      'not in expected format: {}', err)
        return self._address

    @property
    def project(self):
        """Determine the Kubernetes project.

        :returns: Project name string.
        """
        if not self._project:
            nodeitem = list(self._cluster.nodes)[0]
            try:
                self._project = re.search(
                    r'//([a-zA-Z0-9-]+)/',
                    nodeitem.spec()['providerID']).group(1)
            except KeyError:
                self._project = 'Cluster-' + nodeitem.spec()['externalID']
        return self._project

    def find_cluster_entity(self):
        """Provide the Kubernetes Cluster entity."""
        try:
            yield self.create_entity()
        except requests.ConnectionError:
            if not self._logged_k8s_unreachable:
                log.info('Kubernetes API server unreachable')
                self._logged_k8s_unreachable = True
        else:
            self._logged_k8s_unreachable = False

    def create_entity(self):
        """Generator of Kubernetes Cluster Entity."""
        update = entityd.EntityUpdate('Kubernetes:Cluster')
        update.label = self.project
        update.attrs.set('kubernetes:kind', 'Cluster')
        update.attrs.set('kubernetes:cluster', self.address, {'entity:id'})
        return update
