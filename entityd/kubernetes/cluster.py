"""Plugin providing Kubernetes Cluster entity."""

import re
import requests

import kube
import logbook

import entityd.pm


log = logbook.Logger(__name__)


class ClusterEntity:
    """Plugin to generate Kubernetes Cluster Entities."""

    def __init__(self):
        self.session = None
        self._cluster = None
        self._logged_k8s_unreachable = False
        self._cluster_endpoint = None
        self._cluster_name = None

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
    def cluster_endpoint(self):
        """Provide the Kubernetes Cluster endpoint."""
        if self._cluster_endpoint:
            return self._cluster_endpoint
        endpoints = self._cluster.proxy.get(
            'http://localhost:8001/api/v1/endpoints')
        for item in endpoints['items']:
            if item.get('metadata', {}).get('name') == 'kubernetes':
                self._cluster_endpoint = item[
                    'subsets'][0]['addresses'][0]['ip']
                break
        return self._cluster_endpoint

    @property
    def cluster_name(self):
        """Determine the Cluster's name.

        :returns: Cluster name string.
        """
        if not self._cluster_name:
            nodeitem = list(self._cluster.nodes)[0]
            self._cluster_name = re.search(
                r'//([a-z0-9-]+)/',
                nodeitem.raw['spec']['providerID']).group(1)
        return self._cluster_name

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
        update.label = self.cluster_name
        update.attrs.set('kubernetes:kind', 'Cluster')
        update.attrs.set(
            'kubernetes:api_endpoint', self.cluster_endpoint,
            {'entity:id'}
        )
        update.attrs.set('kubernetes:name', self.cluster_name)
        return update
