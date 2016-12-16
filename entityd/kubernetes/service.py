"""Plugin providing Kubernetes Service entities."""

import kube
import logbook
import requests

import entityd.pm
from . import Kutilities


RFC_3339_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
log = logbook.Logger(__name__)


class ServiceEntity:
    """Plugin to generate Kubernetes Service Entities."""

    def __init__(self):
        self.session = None
        self._kutils = None
        self._logged_k8s_unreachable = None

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Register the Service Entity."""
        config.addentity('Kubernetes:Service',
                         'entityd.kubernetes.service.ServiceEntity')

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        """Store the session for later usage."""
        self.session = session
        self._kutils = Kutilities(session)

    @entityd.pm.hookimpl
    def entityd_sessionfinish(self):
        """Safely terminate the plugin."""
        self._kutils.cluster.close()

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs, include_ondemand=False):  # pylint: disable=unused-argument
        """Return an iterator of Kubernetes Service entities."""
        if name == 'Kubernetes:Service':
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.find_services()

    def find_services(self):
        """Find Kubernetes Service entities."""
        try:
            pods = self._kutils.determine_pods_labels()
            for resource in self._kutils.cluster.services:
                yield self.create_entity(resource, pods)
        except requests.ConnectionError:
            if not self._logged_k8s_unreachable:
                log.info('Kubernetes API server unreachable')
                self._logged_k8s_unreachable = True
        else:
            self._logged_k8s_unreachable = False

    def create_entity(self, resource, pods):
        """Create an entity representing a Kubernetes Service.

        :param resource: Kubernetes resource item.
        :type resource: kube._service.ServiceItem
        :param dict pods: Set of labels for each pod in the cluster.
        """
        update = self._kutils.create_base_entity(resource, pods)
        try:
            update.attrs.set('kubernetes:load', resource.loadbalancer_ingress)
        except kube.StatusError:
            pass
        return update
