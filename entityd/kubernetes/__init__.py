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
        self.logged_k8s_unreachable = False

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
        """Finds and yields Kubernetes entities.

        :returns: A generator of :class:`entityd.EntityUpdate`s.
        """

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
        """Create the UEID for a pod.

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

    def create_replicaset_ueid(self, rs_name, namespace):
        """Create the ueid for a replicaset.

        :param str rs_name: Replicaset's name.
        :param str namespace: Replicaset's namespace.

        :returns: A :class:`cobe.UEID` for the Replica Set.
        """
        update = entityd.EntityUpdate('Kubernetes:ReplicaSet')
        update.attrs.set('kubernetes:meta:name', rs_name, traits={'entity:id'})
        update.attrs.set(
            'kubernetes:meta:namespace', namespace, traits={'entity:id'})
        update.attrs.set(
            'cluster', str(self.cluster_ueid), traits={'entity:id'})
        return update.ueid

    def create_labelselector(self, resource):
        """Create the `labelSelector` string from resource's selector labels.

        This is suitable for those resources having
        `spec.selector.matchLabels` and `spec.selector.matchExpressions` -
        e.g. suitable for Replica Sets, but not Services.

        Note that operators from ``spec.selector.matchExpressions`` are
        converted to lower case. This with some versions of Kubernetes
        it seems to be possible to specify an expression using mixed
        case operators such as ``NotIn`` which are then not usable in
        label selectors.

        :param resource: Kubernetes resource item.
        :type resource: kube.{resource_type}.{resource_type}Item

        :returns: `labelSelector` string.
        """
        try:
            matchlabels = resource.spec()['selector']['matchLabels']
        except KeyError:
            matchlabels = {}
        matchlabels = ','.join(
            '{}={}'.format(k, v) for k, v in matchlabels.items())
        try:
            matchexpressions = resource.spec()['selector']['matchExpressions']
        except KeyError:
            matchexpressions = []
        expressions = []
        for expression in matchexpressions:
            values = ' '.join(
                ['{}'.format(item) for item in expression['values']])
            expressions.append('{} {} ({})'.format(
                expression['key'], expression['operator'].lower(), values))
        matchexpressions = ','.join(expressions)
        labelselector = ','.join([matchlabels, matchexpressions]).strip(',')
        return labelselector

    def find_resource_pod_children(self, resource, api_path):
        """Find the set of pod UEIDs that are the children of a resource.

        This is suitable for those resources having pods as children and with
        `spec.selector.matchLabels` and `spec.selector.matchExpressions` -
        e.g. suitable for Replica Sets, but not Services, which have
        their own method.

        :param resource: Kubernetes resource item.
        :type resource: kube.{resource_type}.{resource_type}Item,
            as described above; e.g. kube._replicaset.ReplicaSetItem
        :param str api_path: The k8s API base path to find pods.

        :returns: Set of instances of :class:`cobe.UEID` for pods that are
            the children of the resource.
        """
        labelselector = self.create_labelselector(resource)
        pods = self.find_pods_by_labels(
            resource.meta.namespace, labelselector, api_path)
        return pods

    def find_deployment_rs_children(self, deployment, api_path):
        """Find the set of rs UEIDs that are the children of a deployment.

        :param deployment: Kubernetes deployment.
        :type deployment: :class:`kube._deployment.DeploymentItem`
        :param str api_path: The k8s API base path to find replica sets.

        :returns: Set of instances of :class:`cobe.UEID` for replica sets
            that are the children of the deployment.
        """
        params = {'labelSelector': self.create_labelselector(deployment)}
        response = self.cluster.proxy.get(
            '{}/namespaces/{}/replicasets'.format(
                api_path, deployment.meta.namespace), **params)
        replicasets = set()
        for rawrs in response['items']:
            rsitem = kube.ReplicaSetItem(self.cluster, rawrs)
            replicasets.add(
                self.create_replicaset_ueid(
                    rsitem.meta.name, deployment.meta.namespace))
        return replicasets

    def find_service_or_rc_pod_children(self, resource, api_path):
        """Find the set of pod UEIDs that are the children of a Service or RC.

        :param resource: Kubernetes Service or Replication Controller resource.
        :type resource: kube._service.ServiceItem |
            kube._replicationcontroller.ReplicationControllerItem
        :param str api_path: The k8s API base path.
        """

        try:
            labels = resource.spec()['selector']
        except KeyError:
            labels = {}
        labelselector = ','.join(
            '{}={}'.format(k, v) for k, v in labels.items())
        return self.find_pods_by_labels(
            resource.meta.namespace, labelselector, api_path)

    def find_pods_by_labels(self, namespace, labelselector, api_path):
        """Find set of pod UEIDs in a namespace from a labelselector.

        :param str namespace: Name of the namespace pods are in.
        :param str labelselector: labelSelector string.
        :param str api_path: The k8s API base path to find pods.
        """
        params = {'labelSelector': labelselector}
        response = self.cluster.proxy.get(
            '{}/namespaces/{}/pods'.format(api_path, namespace), **params)
        pods = set()
        for rawpod in response['items']:
            poditem = kube.PodItem(self.cluster, rawpod)
            pods.add(self.create_pod_ueid(poditem.meta.name, namespace))
        return pods

    def log_api_server_unreachable(self):
        """Log once that the Kubernetes API server is unreachable."""
        if not self.logged_k8s_unreachable:
            log.info('Kubernetes API server unreachable')
            self.logged_k8s_unreachable = True

    def create_base_entity(self, resource, children):
        """Creator of the base entity for certain Kubernetes resources.

        This provides the base entity for Replica Sets, Services,
        Replication Controllers, Deployments and Daemonsets.

        The labels of the resource that correspond (as a subset) to those of
        the resource's children are obtained from the resource's template,
        other than for a Service, where the Service metadata labels are used.

        :param resource: Kubernetes resource item.
        :type resource: kube.ReplicaSetItem | kube.ServiceItem
        :param set children: Set of UEIDs that are the children of
            the resource.
        """
        meta = resource.meta
        kind = resource.kind.value
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
        for child in children:
            update.children.add(child)
        update.parents.add(self.create_namespace_ueid(meta.namespace))
        return update
