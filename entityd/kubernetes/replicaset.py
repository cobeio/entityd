"""Plugin providing Kubernetes Replica Set entities."""

import kube
import logbook
import requests

import entityd.pm
from . import Kutilities


RFC_3339_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
log = logbook.Logger(__name__)


class ReplicaSetEntity:
    """Plugin to generate Kubernetes Replica Set Entities."""

    def __init__(self):
        self.session = None
        self._kutils = None
        self._logged_k8s_unreachable = None

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Register the Replica Set Entity."""
        config.addentity('Kubernetes:ReplicaSet',
                         'entityd.kubernetes.replicaset.ReplicaSetEntity')

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
        """Return an iterator of Kubernetes Replica Set entities."""
        if name == 'Kubernetes:ReplicaSet':
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.find_replicasets()

    def find_replicasets(self):
        """Find Kubernetes Replica Set entities."""
        try:
            pods = self._kutils.determine_pods_labels()
            for resource in self._kutils.cluster.replicasets:
                yield self.create_entity(resource, pods)
        except requests.ConnectionError:
            if not self._logged_k8s_unreachable:
                log.info('Kubernetes API server unreachable')
                self._logged_k8s_unreachable = True
        else:
            self._logged_k8s_unreachable = False

    def create_entity(self, resource, pods):
        """Create an entity representing a Kubernetes Replica Set.

        :param resource: Kubernetes resource item.
        :type resource: kube._replicaset.ReplicaSetItem
        :param dict pods: Set of labels for each pod in the cluster.
        """
        update = self._kutils.create_base_entity(resource, pods)
        attributes = {
            'kubernetes:observed-replicas': 'observed_replicas',
            'kubernetes:observed-generation': 'observed_generation',
            'kubernetes:fully-labeled-replicas': 'fully_labeled_replicas',
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
        return update
