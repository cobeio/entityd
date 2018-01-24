"""Plugin providing Kubernetes node entities.

This module implements the Kubernetes node entity.
An entity update of the same UEID will be created by the ``entityd.hostme``
plugin, with a different set of properties and attributes. The entity
in the model therefore comprises the properties and attributes from both
plugins.
"""

import collections
import datetime

import kube
import logbook
import requests

import entityd.kubernetes
import entityd.pm


log = logbook.Logger(__name__)
Poddata = collections.namedtuple('Poddata', ['name', 'namespace'])


class NodeEntity:
    """Plugin to generate Kubernetes Node Entities."""

    def __init__(self):
        self.session = None
        self._cluster = None
        self._cluster_ueid = None
        self._logged_k8s_unreachable = None


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
    def entityd_emit_entities(self):
        """Generate all Kubernetes "Node" entity updates."""
        yield from self.nodes()

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

    def determine_pods_on_nodes(self):
        """Determine the name and namespace of the pods on each node.

        Note that any pods not yet assigned to a node are skipped by handling
        the KeyError that occurs under such circumstances.

        :returns: A dict of form:
            {nodename: set(named tuples for node's pods
                           with fields 'name' and 'namespace'),
            ...}
        """
        pods_on_nodes = collections.defaultdict(set)
        for pod in self._cluster.pods:
            pod_name = pod.meta.name
            pod_namespace = pod.meta.namespace
            try:
                node_name = pod.spec()['nodeName']
            except KeyError:
                continue
            pod_entry = Poddata(name=pod_name, namespace=pod_namespace)
            pods_on_nodes[node_name].add(pod_entry)
        return pods_on_nodes

    def nodes(self):
        """Provide all the Kubernetes node entities."""
        try:
            pods_on_nodes = self.determine_pods_on_nodes()
            for node in self._cluster.nodes:
                node_entity = self.create_entity(node, pods_on_nodes)
                if node.raw['spec'].get('unschedulable'):
                    observation_entity = \
                        self.create_cordoned_observation(str(node_entity.ueid))
                    observation_entity.children.add(node_entity)
                    yield observation_entity
                for condition in list(node.raw['status']['conditions']):
                    if condition['type'] == 'Ready'\
                            and condition['status'] != 'True':
                        observation_entity = \
                            self.create_not_ready_observation(
                                node,
                                str(node_entity.ueid),
                            )
                        observation_entity.children.add(node_entity)
                        yield observation_entity
                yield node_entity
        except requests.ConnectionError:
            if not self._logged_k8s_unreachable:
                log.info('Kubernetes API server unreachable')
                self._logged_k8s_unreachable = True
        else:
            self._logged_k8s_unreachable = False

    def create_entity(self, node, nodepods):
        """Creator of Kubernetes Node Entities."""
        meta = node.meta
        node_name = meta.name
        update = entityd.EntityUpdate('Host')
        update.label = node_name
        update.attrs.set('kubernetes:kind', 'Node')
        update.attrs.set(
            'bootid', node.raw['status']['nodeInfo']['bootID'], {'entity:id'})
        update.attrs.set('kubernetes:meta:name', node_name)
        update.attrs.set('kubernetes:meta:version', meta.version)
        update.attrs.set('kubernetes:meta:created',
                         meta.created.strftime(
                             entityd.kubernetes.RFC_3339_FORMAT),
                         traits={'chrono:rfc3339'}
                        )
        update.attrs.set('kubernetes:meta:link', meta.link, traits={'uri'})
        update.attrs.set('kubernetes:meta:uid', meta.uid)
        update.attrs.set('kubernetes:meta:labels', dict(meta.labels))
        for pod_data in nodepods.get(node_name, []):
            update.children.add(self.create_pod_ueid(pod_data.name,
                                                     pod_data.namespace))
        update.parents.add(self.cluster_ueid)
        return update

    def create_cordoned_observation(self, ueid):
        """Creator of Cordoned Node Observation Entities."""
        update = entityd.EntityUpdate('Observation')
        update.label = "Node is cordoned"
        update.attrs.set('kubernetes:node',
                         ueid,
                         traits={'entity:id', 'entity:ueid'},
                        )
        update.attrs.set('observation-type',
                         'cordoned',
                         traits={'entity:id'},
                        )
        update.attrs.set('start',
                         datetime.datetime.now().strftime(
                             entityd.kubernetes.RFC_3339_FORMAT),
                         traits={'chrono:rfc3339'},
                        )
        update.attrs.set('kind', value='Unschedulable', traits=[])
        update.attrs.set('message',
                         value='The node has been cordoned '
                         'and is unschedulable.', traits=[])
        update.attrs.set('hints',
                         value='Uncordon this node '
                               'to make it schedulable.',
                         traits=[])
        update.attrs.set('importance', 2, traits=[])
        update.attrs.set('urgency', 2, traits=[])
        update.attrs.set('certainty', 10, traits=[])
        return update

    def create_not_ready_observation(self, node, ueid):
        """Creator of Not Ready Node Observation Entities."""
        update = entityd.EntityUpdate('Observation')
        update.label = 'Node is not ready'
        update.attrs.set('kubernetes:node',
                         ueid,
                         traits={'entity:id', 'entity:ueid'},
                        )
        update.attrs.set('observation-type', 'notready', traits={'entity:id'})
        update.attrs.set('start',
                         datetime.datetime.now().strftime(
                             entityd.kubernetes.RFC_3339_FORMAT),
                         traits={'chrono:rfc3339'},
                        )
        update.attrs.set('kind', value='NotReady', traits=[])
        update.attrs.set('message',
                         value='The node is not ready.',
                         traits=[],
                        )
        update.attrs.set('hints',
                         value="Check the node's condition"
                               "attributes to discover why.",
                         traits=[],
                        )
        update.attrs.set('importance', 6, traits=[])
        update.attrs.set('urgency', 6, traits=[])
        update.attrs.set('certainty', 10, traits=[])
        for condition in node.raw['status']['conditions']:
            update.attrs.set('condition:' + condition['type'],
                             list(condition),
                            )
        return update
