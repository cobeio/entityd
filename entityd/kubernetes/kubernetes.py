"""Plugin providing entities for Kubernetes.

This module implements all the entities for various Kubernetes
components. Each entity type is implemented as a generator function.
A single ``entityd_find_entity`` hook implementation takes responsibility
for dispatching to the correct generator function.
"""

import datetime
import collections

import kube
import logbook
import requests

import entityd.kubernetes
import entityd.pm
from entityd.docker.container import DockerContainer

log = logbook.Logger(__name__)
_LOGGED_K8S_UNREACHABLE = False
_CLUSTER_UEID = None
ENTITIES_PROVIDED = {
    'Kubernetes:Container': 'generate_containers',
    'Kubernetes:Namespace': 'generate_namespaces',
    'Kubernetes:Pod': 'generate_pods',
    'Kubernetes:Pod:Probe': 'generate_probes',
    # 'Observation': 'generate_probe_observations',
}
Point = collections.namedtuple('Point', ('timestamp', 'data'))
Point.__doc__ = """Container statistics at a point in time.

:ivar datetime.datetime timestamp: the UTC timestamp for the data point.
:ivar dict data: the container statistics as returned by cAdvisor for the
    corresponding timestamp.
"""


class Metric:
    """Represents a metric for a container.

    :param str name: the name of the attribute that is set on the entity
        update for the metric.
    :param tuple path: a sequence of object keys that will be used to
        traverse a cAdvisor JSON response to find the metric value. E.g.
        a path of ``('A', 'B')`` is effectively ``response['A']['B']``.
    :param Set[str] traits: a set of string traits to set for the metric's
        attribute.
    """

    def __init__(self, name, path, traits):
        self._name = str(name)
        self._path = tuple(path)
        self._traits = frozenset(str(trait) for trait in traits)

    def __repr__(self):
        return '<{0} {1} @ {2} traits: {3}>'.format(
            self.__class__.__name__,
            self._name,
            '.'.join(str(s) for s in self._path),
            ', '.join(sorted(self._traits)),
        )

    def with_prefix(self, prefix, path):
        """Get new :class:`Metric` with the name prefixed.

        :param str prefix: the prefix to use for the metric name.
        :param tuple path: the value path prefix.

        :returns: a new :class:`Metric` with the same traits but
            the name and path prefixed.
        """
        return self.__class__(
            prefix + ':' + self._name, path + self._path, self._traits)

    def transform(self, value):  # pylint: disable=no-self-use
        """Transform metric value to a normalised form.

        By default this returns the value as-is. Subclasses should override
        this to modify how metric values are interpreted.
        """
        return value

    def apply(self, object_, update):
        """Apply the metric from an data point to an entity update.

        This will attempt to find the metric value from a given object by
        walking the path to the metric. If a value is found it will be
        transformed according to :meth:`transform` and then set as an
        attribute on the given update.

        If the metric value couldn't be found then the attribute is deleted
        on the update.

        :param object_: a :class:`Point`'s' data object to lookup the
            metric from.
        :param entityd.EntityUpdate update: the update to set the
            attribute on.
        """
        value = object_
        for step in self._path:
            try:
                value = value[step]
            except KeyError:
                log.debug(
                    'Could not determine value for metric {}'.format(self))
                update.attrs.delete(self._name)
                return
        update.attrs.set(self._name, self.transform(value), self._traits)


class NanosecondMetric(Metric):
    """Convert nanosecond metric values to seconds."""

    def transform(self, value):
        return value / (10 ** 9)


class MillisecondMetric(Metric):
    """Convert millisecond metric values to seconds."""

    def transform(self, value):
        return value / (10 ** 6)


@entityd.pm.hookimpl
def entityd_configure(config):
    """Configure Kubernetes entities.

    This registers all the entities implemented by this module.
    """
    for entity_type in ENTITIES_PROVIDED:
        config.addentity(entity_type, __name__)


@entityd.pm.hookimpl
def entityd_find_entity(name, attrs=None,
                        include_ondemand=False, session=None):  # pylint: disable=unused-argument
    """Find Kubernetes entities.

    :raises LookupError: if ``attrs`` is given.
    """
    if name in ENTITIES_PROVIDED:
        if attrs is not None:
            raise LookupError('Attribute based filtering not supported')
        return generate_updates(globals()[ENTITIES_PROVIDED[name]], session)


def get_cluster_ueid(session):
    """Get the Kubernetes Cluster UEID.

    :raises LookupError: If a Cluster UEID cannot be found.

    :returns: A :class:`cobe.UEID` for the Cluster.
    """
    global _CLUSTER_UEID  # pylint: disable=global-statement
    if _CLUSTER_UEID:
        return _CLUSTER_UEID
    results = session.pluginmanager.hooks.entityd_find_entity(
        name='Kubernetes:Cluster', attrs=None)
    for result in results:
        if result:
            for cluster_entity in result:
                _CLUSTER_UEID = cluster_entity.ueid
                return cluster_entity.ueid
    raise LookupError('Could not find the Cluster UEID')


def generate_updates(generator_function, session):
    """Wrap an entity update generator function.

    This function wraps around any entity update generator and
    manages the :class:`kube.Cluster`'s life-time and creation
    of :class:`entityd.EntityUpdate`s.

    When the generator function is initially called it is
    passed a :class:`kube.Cluster`. Then it is continually sent
    :class:`entityd.EntityUpdate`s until the generator is exhausted.

    Any :exc:`kube.StatusError` raised from the generator function
    are caught and logged. The update that resulted in the exception
    will not be returned and no further updates will be retrieved from
    inner generator.

    :param generator_function: a generator function that yields
        :class:`entityd.EntityUpdate`s.

    :raises KeyError: if the given generator function's ``__name__`` is
        not registered in :data:`ENTITIES_PROVIDED`.

    :returns: a generator of the updates returned by ``generator_function``.
    """
    global _LOGGED_K8S_UNREACHABLE  # pylint: disable=global-statement
    name = {value: key for key, value
            in ENTITIES_PROVIDED.items()}[generator_function.__name__]
    with kube.Cluster() as cluster:
        try:
            generator = generator_function(cluster, session)
            next(generator)
            while True:
                update = entityd.EntityUpdate(name)
                update.exception = False
                try:
                    generator.send(update)
                except StopIteration:
                    # todo: apply better approach to this, as described below
                    if not update.exception:
                        del update.exception
                        yield update
                    break
                except kube.StatusError:
                    log.exception('Unexpected status error')
                    break
                else:
                    # todo: same here
                    if not update.exception:
                        del update.exception
                        yield update
                    else:
                        # included purely for pytest test & coverage purposes
                        assert update.exception

        except requests.ConnectionError:
            if not _LOGGED_K8S_UNREACHABLE:
                log.info('Kubernetes API server unreachable')
                _LOGGED_K8S_UNREACHABLE = True
        else:
            _LOGGED_K8S_UNREACHABLE = False


def apply_meta_update(meta, update, session):
    """Apply update attributes for a :class:`kube.ObjectMeta`.

    This sets attributes on the ``update`` that match the corresponding
    values on the given ``meta`` object. Each attribute name will be
    the same as on the meta object but prefixed by ``meta:``.

    The meta object's name and namespace (if set for the given object meta)
    become ``id``-typed attributes.

    :param kube.ObjectMeta meta: the meta object to set attributes for.
    :param entityd.EntityUpdate update: the update to apply the attributes to.
    """
    update.attrs.set('kubernetes:meta:name', meta.name, {'entity:id', 'index'})
    update.attrs.set('kubernetes:meta:labels', dict(meta.labels))
    if meta.namespace:
        update.attrs.set('kubernetes:meta:namespace',
                         meta.namespace, {'entity:id', 'index'})
        namespace_group_ueid = \
            entityd.kubernetes.NamespaceGroup.get_ueid(meta.namespace, session)
        update.parents.add(namespace_group_ueid)
    else:
        update.attrs.delete('kubernetes:meta:namespace')
    update.attrs.set('cluster', str(get_cluster_ueid(session)),
                     traits={'entity:id', 'entity:ueid'})
    update.attrs.set('kubernetes:meta:version', meta.version)
    update.attrs.set(
        'kubernetes:meta:created',
        meta.created.strftime(entityd.kubernetes.RFC_3339_FORMAT),
        traits={'chrono:rfc3339'},
    )
    # TODO: Maybe convert to absolute URI
    update.attrs.set('kubernetes:meta:link', meta.link, traits={'uri'})
    update.attrs.set('kubernetes:meta:uid', meta.uid)


def generate_namespaces(cluster, session):
    """Generate updates for namespaces."""
    for namespace in cluster.namespaces:
        namespace_update(namespace, (yield), session)


def namespace_update(namespace, update, session):
    """Populate update with attributes for a namespace.

    This will apply metadata attributes as well as a ``phase`` attribute
    indicating the phase of the namespace.

    :param kube.Container container: the container to set attributes for.
    :param entityd.EntityUpdate update: the update to set the attributes on.
    """
    update.label = namespace.meta.name
    apply_meta_update(namespace.meta, update, session)
    update.attrs.set(
        'phase',
        namespace.phase.value,
        {'kubernetes:namespace-phase', 'index'},
    )
    update.parents.add(get_cluster_ueid(session))


def generate_pods(cluster, session):
    """Generate updates for pods."""
    # TODO: Set parent to namespace if pod has no controller
    for pod in cluster.pods:
        update = yield
        pod_update(pod, update, session)


def pod_update(pod, update, session):
    """Populate update with attributes for a pod.

    :param kube.Pod pod: the pod to set attributes for.
    :param entityd.EntityUpdate update: the update to set the attributes on.

    :returns: the ``update`` with additional attributes.
    """
    update.label = pod.meta.name
    apply_meta_update(pod.meta, update, session)
    update.attrs.set('kubernetes:kind', 'Pod')
    update.attrs.set(
        'phase', pod.phase.value, {'kubernetes:pod-phase', 'index'})
    update.attrs.set('start_time',
                     pod.start_time.strftime(
                         entityd.kubernetes.RFC_3339_FORMAT),
                     traits={'chrono:rfc3339'})
    try:
        update.attrs.set('ip', str(pod.ip),
                         {'ipaddr:v{}'.format(pod.ip.version), 'index'})
    except kube.StatusError:
        update.attrs.delete('ip')
    for attribute in ('message', 'reason'):
        try:
            value = getattr(pod, attribute)
        except kube.StatusError:
            pass
        else:
            update.attrs.set(attribute, value, {'index'})
    return update


def generate_probes(cluster, session):
    """Generate updates for readiness and liveness probes.

    :returns: a generator of :class:`entityd.EntityUpdate`s.
    """
    for pod_update in generate_updates(generate_pods, session):  # pylint: disable=redefined-outer-name
        try:
            namespace = cluster.namespaces.fetch(
                pod_update.attrs.get('kubernetes:meta:namespace').value)
            pod = cluster.pods.fetch(
                pod_update.attrs.get('kubernetes:meta:name').value,
                namespace=namespace.meta.name
            )
        except LookupError:
            pass
        else:
            pod_ip = pod.raw['status'].get('podIP')
            try:
                liveness_probe = \
                    pod.raw['spec']['containers'][0]['livenessProbe']
            except KeyError:
                pass
            else:
                update = yield
                update.label = 'Liveness:'
                update.attrs.set('pod',
                                 str(pod_update.ueid),
                                 traits={'entity:id', 'entity:ueid'},
                                )
                update.attrs.set('kubernetes:probe:type',
                                 'Liveness probe',
                                 traits={'entity:id'},
                                )
                populate_probe_update(update, liveness_probe, pod_ip)
                update.children.add(pod_update)
            try:
                readiness_probe =\
                    pod.raw['spec']['containers'][0]['readinessProbe']
            except KeyError:
                pass
            else:
                update = yield
                update.label = 'Readiness:'
                update.attrs.set('pod',
                                 str(pod_update.ueid),
                                 traits={'entity:id', 'entity:ueid'},
                                )
                update.attrs.set('kubernetes:probe:type',
                                 'Readiness probe',
                                 traits={'entity:id'},
                                )
                populate_probe_update(update, readiness_probe, pod_ip)
                update.children.add(pod_update)


def populate_probe_update(update, probe, pod_ip):
    """Populate update with attributes for a probe.

    :param entityd.EntityUpdate update: the update to set the attributes on.
    :param probe: a dictionary of probe attributes from Kube:pod
    :param pod_ip: a string of the pod IP address
    """
    update.attrs.set('failure-threshold', probe.get('failureThreshold'))
    update.attrs.set('period-seconds', probe.get('periodSeconds'))
    update.attrs.set('success-threshold', probe.get('successThreshold'))
    update.attrs.set('timeout-seconds', probe.get('timeoutSeconds'))
    update.attrs.set('initial-delay-seconds', probe.get('initialDelaySeconds'))
    probe_exec = probe.get('exec')
    http_get = probe.get('httpGet')
    tcp_socket = probe.get('tcpSocket')
    if probe_exec:
        commands = list(probe_exec['command'])
        if len(commands) > 0:
            update.label += " ".join(commands)
        update.attrs.set('exec:command', commands)
    elif http_get:
        path = http_get.get('path')
        scheme = http_get.get('scheme')
        if path and scheme and pod_ip:
            update.label += scheme + "://" + pod_ip + path
        if path:
            update.attrs.set('httpGet:path', path)
        if scheme:
            update.attrs.set('httpGet:scheme', scheme)
        update.attrs.set('httpGet:port', http_get.get('port'))
    elif tcp_socket:
        update.label += 'port=' + str(tcp_socket['port'])
        update.attrs.set('tcpSocket:port', tcp_socket['port'])


def generate_probe_observations(cluster, session): # pylint: disable=unused-argument
    """Generate updates for probe related observations.

    :returns: a generator of :class:`entityd.EntityUpdate`s.
    """
    events_info = cluster.proxy.get('api/v1/events/')
    event_items = events_info.get('items')
    for event in event_items:
        involved_object = event['involvedObject']
        if involved_object['kind'] == 'Pod' and\
                        event['reason'] == 'Unhealthy' and\
                        event['type'] == 'Warning':
            if event['message'].startswith('Liveness probe failed'):
                probe_type = 'Liveness probe'
            elif event['message'].startswith('Readiness probe failed'):
                probe_type = 'Readiness probe'
            else:
                return
            pod_name = involved_object['name']
            try:
                pod_ueid = create_pod_ueid(pod_name, cluster)
            except LookupError as err:
                log.info(err)
                return
            probe_ueid = create_probe_ueid(pod_ueid, probe_type)
            update = yield
            meta = event['metadata']
            count = event['count']
            update.label = str(count) + ' probe failure(s)'
            update.attrs.set('kubernetes:event:name',
                             meta['name'],
                             traits={'entity:id'},
                            )
            update.attrs.set('kubernetes:event:firstTimestamp',
                             event['firstTimestamp'],
                             traits={'chrono:rfc3339'},
                            )
            update.attrs.set('kubernetes:event:lastTimestamp',
                             event['lastTimestamp'],
                             traits={'chrono:rfc3339'},
                            )
            update.attrs.set('kubernetes:event:count', event['count'])
            update.attrs.set('kind', value=probe_type + ' failure', traits=[])
            update.attrs.set('message', value=event['message'], traits=[])
            update.attrs.set('hints',
                             value='See message for details.',
                             traits=[],
                            )
            update.attrs.set('importance', 3, traits=[])
            update.attrs.set('urgency', 2, traits=[])
            update.attrs.set('certainty', 10, traits=[])
            update.children.add(pod_ueid)
            update.children.add(probe_ueid)


def create_pod_ueid(pod_name, cluster):
    """Create the ueid for a pod.

    :param str podname: Pod's name.
    :param str namespace: Pod's namespace.

    :returns: A :class:`cobe.UEID` for the pod.
    """
    for pod in cluster.pods:
        if pod_name == pod.meta.name:
            update = entityd.EntityUpdate('Kubernetes:Pod')
            update.attrs.set('kubernetes:meta:name', pod_name,
                             traits={'entity:id'})
            update.attrs.set(
                'kubernetes:meta:namespace', pod.meta.namespace,
                traits={'entity:id'})
            update.attrs.set(
                'cluster', _CLUSTER_UEID, traits={'entity:id'})
            return update.ueid
    raise LookupError("Pod {} not found in the cluster".format(pod_name))


def create_probe_ueid(pod_ueid, probe_type):
    """Create the ueid for a probe.

    :param pod_ueid: The UEID of the pod associated with the probe.
    :type pod_ueid: cobe.UEID
    :param str probe_type: 'Liveness probe' or 'Readiness probe'

    :returns: A :class:`cobe.UEID` for the probe.
    """
    update = entityd.EntityUpdate('Kubernetes:Pod:Probe')
    update.attrs.set('pod',
                     str(pod_ueid),
                     traits={'entity:id', 'entity:ueid'},
                    )
    update.attrs.set('kubernetes:probe:type', probe_type, traits={'entity:id'})
    return update.ueid


def generate_containers(cluster, session):
    """Generate updates for containers.

    :returns: a generator of :class:`entityd.EntityUpdate`s.
    """
    for pod_update in generate_updates(generate_pods, session):  # pylint: disable=redefined-outer-name
        try:
            namespace = cluster.namespaces.fetch(
                pod_update.attrs.get('kubernetes:meta:namespace').value)
            pod = cluster.pods.fetch(
                pod_update.attrs.get('kubernetes:meta:name').value,
                namespace=namespace.meta.name
            )
        except LookupError:
            pass
        else:
            for container in pod.containers:
                update = yield
                try:
                    update.parents.add(pod_update)
                    container_metrics(container, update)
                    container_update(container, pod, update, session)
                # todo: tidy this approach to handling no containerId from kube
                except KeyError as err:
                    update.exception = True
                    log.info('KeyError, likely due to container '
                             'having no containerID: {}'.format(err))
                else:
                    update.exception = False


def container_update(container, pod, update, session):
    """Populate update with attributes for a container.

    :param kube.Container container: the container to set attributes for.
    :param kube.Pod pod: the pod the container is within.
    :param entityd.EntityUpdate update: the update to set the attributes on.
    """
    update.label = container.name
    update.attrs.set('id', container.id, {'entity:id', 'index'})
    update.attrs.set('name', container.name)
    update.attrs.set('kubernetes:kind', 'Container')
    update.attrs.set('manager', 'Docker')
    update.attrs.set('ready', container.ready)
    update.attrs.set('image:id', container.image_id, {'index'})
    update.attrs.set('image:name', container.image, {'index'})
    update.attrs.set('restart-count', container.restart_count,
                     {'metric:counter', 'index:numeric'})
    for state in ('running', 'waiting', 'terminated'):
        if getattr(container.state, state):
            update.attrs.set('state', state, {'index'})
    if container.state.running or container.state.terminated:
        update.attrs.set(
            'state:started-at',
            container.state.started_at.strftime(
                entityd.kubernetes.RFC_3339_FORMAT),
            traits={'chrono:rfc3339'},
        )
    else:
        update.attrs.delete('state:started-at')
    if container.state.waiting or container.state.terminated:
        update.attrs.set('state:reason', container.state.reason, {'index'})
    else:
        update.attrs.delete('state:reason')
    if container.state.terminated:
        update.attrs.set('state:exit-code', container.state.exit_code)
        try:
            update.attrs.set('state:signal', container.state.signal)
        except kube.StatusError:
            update.attrs.delete('state:signal')
        try:
            update.attrs.set(
                'state:message', container.state.message, {'index'})
        except kube.StatusError:
            update.attrs.delete('state:message')
        update.attrs.set(
            'state:finished-at',
            container.state.finished_at.strftime(
                entityd.kubernetes.RFC_3339_FORMAT),
            traits={'chrono:rfc3339'},
        )
    else:
        for attribute in ('exit-code', 'signal', 'message', 'finished-at'):
            update.attrs.delete('state:' + attribute)

    runtime, container_id = container.id.split('://', 1)
    if runtime == 'docker':
        update.children.add(DockerContainer.get_ueid(container_id))

    namespace_group_ueid = entityd.kubernetes.group.NamespaceGroup.get_ueid(
        pod.meta.namespace, session)
    update.parents.add(namespace_group_ueid)

    container_resources(container, pod, update)


def container_resources(container, pod, update):
    """Add the container compute resource limits and requests.

    :param kube.Container container: the container to set attributes for.
    :param kube.Pod pod: the pod the container is within.
    :param entityd.EntityUpdate update: the update to set the attributes on.
    """
    for cont in pod.raw.spec.containers:
        if cont.name == container.name:
            resources = cont.resources
            break
    else:
        resources = {}
    for name, path, convert, traits in METRICS_CONTAINER_RESOURCES:
        value_raw = None
        object_ = resources
        for part in path:
            if part not in object_:
                break
            object_ = object_[part]
        else:
            value_raw = object_
        if value_raw is not None:
            try:
                value = convert(str(value_raw))
            except ValueError as exception:
                log.error('Error converting {!r} value: {}', name, exception)
            else:
                update.attrs.set(name, value, traits)


def select_nearest_point(target, points, threshold):
    """Select data point nearest to a given point in time.

    :param datetime.datetime target: the target timestamp for the point.
    :param points: an iterator of :class:`Point`s to search.
    :param float threshold: the maximum number of seconds the selected
        point can be away from the target timestamp.

    :raises ValueError: if the selected data point exceeds the threshold.

    :returns: the :class:`Point` nearest to ``target``.
    """
    sorted_points = sorted(
        points, key=lambda p: abs(target - p.timestamp))
    if not sorted_points:
        raise ValueError('No points given')
    if abs(target - sorted_points[0].timestamp).total_seconds() > threshold:
        raise ValueError(
            'No metric point within {} seconds'.format(threshold))
    return sorted_points[0]


def cadvisor_to_points(raw_points):
    """Convert cAdvisor response to :class:`Point`s.

    The timestamp for each data point in the parsed into a UTC
    :class:`datetime.datetime`. Note that cAdvisor sometimes returns
    timestamps with second fractions which are too long to be parsed by
    :meth:`datetime.datetime.strptime`, so they are truncated to six digits.

    If any of the raw points have timestamps which cannot be parsed then
    the point is skipped with a warning logged.

    :param list raw_points: the raw JSON objects as returned by cAdvisor
        for a specific container.

    :returns: a list of :class:`Point`s.
    """
    points = []
    for point in raw_points:
        date_and_time, fraction_and_offset = point['timestamp'].split('.')
        for offset_separator in ('Z', '+', '-'):
            if offset_separator in fraction_and_offset:
                fraction, raw_offset = \
                    fraction_and_offset.split(offset_separator)
                fraction = fraction[:6]
                if raw_offset:
                    hours, minutes = raw_offset.split(':', 1)
                    offset = datetime.timedelta(
                        hours=int(hours), minutes=int(minutes))
                    if offset_separator == '+':
                        offset = -offset
                else:
                    offset = datetime.timedelta()
                break
        else:
            log.warning('Skipping point with unparsable '
                        'timestamp {!r}', point['timestamp'])
            continue
        normalised_datetime = date_and_time + '.' + fraction
        timestamp = datetime.datetime.strptime(
            normalised_datetime, '%Y-%m-%dT%H:%M:%S.%f') + offset
        points.append(Point(timestamp, point))
    return points


def simple_metrics(point, update):
    """Apply :data:`METRICS_CONTAINER` to an update.

    :param Point point: the data point to use for metric values.
    :param entityd.EntityUpdate update: the update to apply the metric
        attributes to.
    """
    for metric in METRICS_CONTAINER:
        metric.apply(point.data, update)


def filesystem_metrics(point, update):
    """Apply filesystem metrics to an update.

    Each filesystem is identified by its UUID which as taken from the
    ``device`` field. The UUID is used to form the attribute prefix
    ``filesystem:{uuid}``.

    :param Point point: the data point to use for metric values.
    :param entityd.EntityUpdate update: the update to apply the metric
        attributes to.
    """
    for index, filesystem in enumerate(point.data.get('filesystem', [])):
        uuid = filesystem['device'].rsplit('/', 1)[-1]
        prefix = 'filesystem:' + uuid
        for filesystem_metric in METRICS_FILESYSTEM:
            filesystem_metric.with_prefix(
                prefix, ('filesystem', index)).apply(point.data, update)


def diskio_metrics(point, update):
    """Apply disk IO metrics to an update.

    Given a container cAdvisor data point, this will look at all sub-fields
    in the ``diskio`` top-level field. Each sub-field will contain a number
    of devices and their corresponding statistics in an array. Each device
    is a JSON object identified by the ``major`` and ``minor`` fields.
    The remaining fields are translated into attributes as per
    :data:`METRICS_DISKIO`.

    For each device, each attribute is prefixed with the
    ``io:{major}:{minor}`` namespace where 'major' and 'minor' identify
    the device as described above.

    :param Point point: the data point to use for metric values.
    :param entityd.EntityUpdate update: the update to apply the metric
        attributes to.
    """
    diskio = point.data.get('diskio', {})
    for key, metrics in METRICS_DISKIO.items():
        if key not in diskio:
            continue
        for index, device in enumerate(diskio[key]):
            major = device['major']
            minor = device['minor']
            for metric in metrics:
                prefix = 'io:{}:{}'.format(major, minor)
                path = ('diskio', key, index)
                metric.with_prefix(prefix, path).apply(point.data, update)


def container_metrics(container, update):
    """Apply container metrics to an update.

    This searches the Kubernetes cluster for cAdvisors listening on each
    node's 4194 port to determine which node hosts the given container.
    Once the correct node is then found, the stats returned by cAdvisor for
    the container are converted to attributes on the entity update.

    As cAdvisor returns a range of stats for a container (a minutes worth
    at one second intervals), the closest matching data point is used for
    the metrics. If there is no data point within 20 minutes, then no
    metrics will be added to the update, to avoid stale metrics.

    If no node can be found for the container then no metrics are added.

    :param kube.Container container: the container to apply metrics for.
    :param entityd.EntityUpdate update: the entity update to apply the
        metric attributes to.
    """
    cluster = container.pod.cluster
    now = datetime.datetime.utcnow()
    container_id = container.id[len('docker://'):]
    for node in cluster.nodes:
        try:
            # TODO: See if it's possible to request a smaller range of values.
            response = cluster.proxy.get(
                'api/v1/proxy/nodes', node.meta.name + ':4194',
                'api/v2.0/stats', container_id, type='docker')
        except kube.APIError as exc:
            pass
        else:
            points_raw = []
            for points_raw_group in response.values():
                points_raw.extend(points_raw_group)
            try:
                points = cadvisor_to_points(points_raw)
            except KeyError:
                points = []
            try:
                point = select_nearest_point(now, points, 20.0 * 60)
            except ValueError as exc:
                log.warning(
                    '{} for container with ID {}'.format(exc, container_id))
            else:
                simple_metrics(point, update)
                filesystem_metrics(point, update)
                diskio_metrics(point, update)
            return
    log.warning(
        'Could not find node for container with ID {}'.format(container_id))


METRICS_CONTAINER_RESOURCES = [  # [(name, path, converter, traits), ...]
    (
        'resources:requests:memory',
        ['requests', 'memory'],
        entityd.kubernetes.ram_conversion,
        {'unit:bytes'},
    ),
    (
        'resources:requests:cpu',
        ['requests', 'cpu'],
        entityd.kubernetes.cpu_conversion,
        {'unit:percent'},
    ),
    (
        'resources:limits:memory',
        ['limits', 'memory'],
        entityd.kubernetes.ram_conversion,
        {'unit:bytes'},
    ),
    (
        'resources:limits:cpu',
        ['limits', 'cpu'],
        entityd.kubernetes.cpu_conversion,
        {'unit:percent'},
    ),
]


METRICS_CONTAINER = [
    NanosecondMetric(
        'cpu:cumulative:total',
        ('cpu', 'usage', 'total'),
        {'metric:counter', 'unit:seconds', 'time:duration'},
    ),
    NanosecondMetric(
        'cpu:cumulative:user',
        ('cpu', 'usage', 'user'),
        {'metric:counter', 'unit:seconds', 'time:duration'},
    ),
    NanosecondMetric(
        'cpu:cumulative:system',
        ('cpu', 'usage', 'system'),
        {'metric:counter', 'unit:seconds', 'time:duration'},
    ),
    NanosecondMetric(
        'cpu:total',
        ('cpu_inst', 'usage', 'total'),
        {'metric:gauge', 'unit:percent'},
    ),
    NanosecondMetric(
        'cpu:user',
        ('cpu_inst', 'usage', 'user'),
        {'metric:gauge', 'unit:percent'},
    ),
    NanosecondMetric(
        'cpu:system',
        ('cpu_inst', 'usage', 'system'),
        {'metric:gauge', 'unit:percent'},
    ),
    Metric(
        'cpu:load-average',
        ('cpu', 'usage', 'load_average'),  # Old path for back compatibility
        {'metric:gauge'},
    ),
    Metric(
        'cpu:load-average',
        ('cpu', 'load_average'),  # New path for load_average
        {'metric:gauge'},
    ),
    Metric(
        'load:sleeping',
        ('load_stats', 'nr_sleeping'),
        {'metric:gauge'},
    ),
    Metric(
        'load:running',
        ('load_stats', 'nr_running'),
        {'metric:gauge'},
    ),
    Metric(
        'load:stopped',
        ('load_stats', 'nr_stopped'),
        {'metric:gauge'},
    ),
    Metric(
        'load:uninterruptible',
        ('load_stats', 'nr_uninterruptible'),
        {'metric:gauge'},
    ),
    Metric(
        'load:io-wait',
        ('load_stats', 'nr_io_wait'),
        {'metric:gauge'},
    ),
    Metric(
        'memory:usage',
        ('memory', 'usage'),
        {'metric:gauge', 'unit:bytes'},
    ),
    Metric(
        'memory:working-set',
        ('memory', 'working_set'),
        {'metric:gauge', 'unit:bytes'},
    ),
    Metric(
        'memory:fail-count',
        ('memory', 'failcnt'),
        {'metric:counter'},
    ),
    Metric(
        'memory:page-fault',
        ('memory', 'container_data', 'pgfault'),
        {'metric:counter'},
    ),
    Metric(
        'memory:page-fault:major',
        ('memory', 'container_data', 'pgmajfault'),
        {'metric:counter'},
    ),
    Metric(
        'network:tcp:established',
        ('network', 'tcp', 'Established'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp:syn-sent',
        ('network', 'tcp', 'SynSent'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp:syn-recv',
        ('network', 'tcp', 'SynRecv'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp:fin-wait-1',
        ('network', 'tcp', 'FinWait1'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp:fin-wait-2',
        ('network', 'tcp', 'FinWait2'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp:time-wait',
        ('network', 'tcp', 'TimeWait'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp:close',
        ('network', 'tcp', 'Close'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp:close-wait',
        ('network', 'tcp', 'CloseWait'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp:last-ack',
        ('network', 'tcp', 'LastAck'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp:listen',
        ('network', 'tcp', 'Listen'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp:closing',
        ('network', 'tcp', 'Closing'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp6:established',
        ('network', 'tcp6', 'Established'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp6:syn-sent',
        ('network', 'tcp6', 'SynSent'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp6:syn-recv',
        ('network', 'tcp6', 'SynRecv'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp6:fin-wait-1',
        ('network', 'tcp6', 'FinWait1'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp6:fin-wait-2',
        ('network', 'tcp6', 'FinWait2'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp6:time-wait',
        ('network', 'tcp6', 'TimeWait'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp6:close',
        ('network', 'tcp6', 'Close'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp6:close-wait',
        ('network', 'tcp6', 'CloseWait'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp6:last-ack',
        ('network', 'tcp6', 'LastAck'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp6:listen',
        ('network', 'tcp6', 'Listen'),
        {'metric:gauge'},
    ),
    Metric(
        'network:tcp6:closing',
        ('network', 'tcp6', 'Closing'),
        {'metric:gauge'},
    ),
]


METRICS_FILESYSTEM = [
    Metric(
        'type',
        ('type',),
        {'index'},
    ),
    Metric(
        'capacity:total',
        ('capacity',),
        {'metric:gauge', 'unit:bytes'},
    ),
    Metric(
        'capacity:usage',
        ('usage',),
        {'metric:gauge', 'unit:bytes'},
    ),
    Metric(
        'capacity:base-usage',
        ('base_usage',),
        {'metric:gauge', 'unit:bytes'},
    ),
    Metric(
        'capacity:available',
        ('available',),
        {'metric:gauge', 'unit:bytes'},
    ),
    Metric(
        'inodes-free',
        ('inodes_free',),
        {'metric:gauge'},
    ),
    Metric(
        'read:completed',
        ('reads_completed',),
        {'metric:counter'},
    ),
    Metric(
        'read:merged',
        ('reads_merged',),
        {'metric:counter'},
    ),
    MillisecondMetric(
        'read:time',
        ('read_time',),
        {'metric:counter', 'unit:seconds', 'time:duration'},
    ),
    Metric(
        'sector:read',
        ('sectors_read',),
        {'metric:counter'},
    ),
    Metric(
        'sector:written',
        ('sectors_written',),
        {'metric:counter'},
    ),
    Metric(
        'write:completed',
        ('writes_completed',),
        {'metric:counter'},
    ),
    Metric(
        'write:merged',
        ('writes_merged',),
        {'metric:counter'},
    ),
    MillisecondMetric(
        'write:time',
        ('write_time',),
        {'metric:counter', 'unit:seconds', 'time:duration'},
    ),
    Metric(
        'io:in-progress',
        ('io_in_progress',),
        {'metric:gauge'},
    ),
    MillisecondMetric(
        'io:time',
        ('io_time',),
        {'metric:counter', 'unit:seconds', 'time:duration'},
    ),
    MillisecondMetric(
        'io:time:weighted',
        ('weighted_io_time',),
        {'metric:gauge', 'unit:seconds', 'time:duration'},
    ),
]


METRICS_DISKIO = {
    'io_service_bytes': [
        Metric(
            'async',
            ('stats', 'Async'),
            {'metric:counter', 'unit:bytes'},
        ),
        Metric(
            'sync',
            ('stats', 'Sync'),
            {'metric:counter', 'unit:bytes'},
        ),
        Metric(
            'read',
            ('stats', 'Read'),
            {'metric:counter', 'unit:bytes'},
        ),
        Metric(
            'write',
            ('stats', 'Write'),
            {'metric:counter', 'unit:bytes'},
        ),
        Metric(
            'total',
            ('stats', 'Total'),
            {'metric:counter', 'unit:bytes'},
        ),
    ],
    'io_serviced': [
        Metric(
            'async:operations',
            ('stats', 'Async'),
            {'metric:counter'},
        ),
        Metric(
            'sync:operations',
            ('stats', 'Sync'),
            {'metric:counter'},
        ),
        Metric(
            'read:operations',
            ('stats', 'Read'),
            {'metric:counter'},
        ),
        Metric(
            'write:operations',
            ('stats', 'Write'),
            {'metric:counter'},
        ),
        Metric(
            'total:operations',
            ('stats', 'Total'),
            {'metric:counter'},
        ),
    ],
    'io_queued': [
        Metric(
            'async:operations:queued',
            ('stats', 'Async'),
            {'metric:counter'},
        ),
        Metric(
            'sync:operations:queued',
            ('stats', 'Sync'),
            {'metric:counter'},
        ),
        Metric(
            'read:operations:queued',
            ('stats', 'Read'),
            {'metric:counter'},
        ),
        Metric(
            'write:operations:queued',
            ('stats', 'Write'),
            {'metric:counter'},
        ),
        Metric(
            'total:operations:queued',
            ('stats', 'Total'),
            {'metric:counter'},
        ),
    ],
    'io_merged': [
        Metric(
            'async:operations:merged',
            ('stats', 'Async'),
            {'metric:counter'},
        ),
        Metric(
            'sync:operations:merged',
            ('stats', 'Sync'),
            {'metric:counter'},
        ),
        Metric(
            'read:operations:merged',
            ('stats', 'Read'),
            {'metric:counter'},
        ),
        Metric(
            'write:operations:merged',
            ('stats', 'Write'),
            {'metric:counter'},
        ),
        Metric(
            'total:operations:merged',
            ('stats', 'Total'),
            {'metric:counter'},
        ),
    ],
    'io_service_time': [
        NanosecondMetric(
            'async:time',
            ('stats', 'Async'),
            {'metric:counter', 'unit:seconds', 'time:duration'},
        ),
        NanosecondMetric(
            'sync:time',
            ('stats', 'Sync'),
            {'metric:counter', 'unit:seconds', 'time:duration'},
        ),
        NanosecondMetric(
            'read:time',
            ('stats', 'Read'),
            {'metric:counter', 'unit:seconds', 'time:duration'},
        ),
        NanosecondMetric(
            'write:time',
            ('stats', 'Write'),
            {'metric:counter', 'unit:seconds', 'time:duration'},
        ),
        NanosecondMetric(
            'total:time',
            ('stats', 'Total'),
            {'metric:counter', 'unit:seconds', 'time:duration'},
        ),
    ],
    'io_wait_time': [
        NanosecondMetric(
            'async:time:wait',
            ('stats', 'Async'),
            {'metric:counter', 'unit:seconds', 'time:duration'},
        ),
        NanosecondMetric(
            'sync:time:wait',
            ('stats', 'Sync'),
            {'metric:counter', 'unit:seconds', 'time:duration'},
        ),
        NanosecondMetric(
            'read:time:wait',
            ('stats', 'Read'),
            {'metric:counter', 'unit:seconds', 'time:duration'},
        ),
        NanosecondMetric(
            'write:time:wait',
            ('stats', 'Write'),
            {'metric:counter', 'unit:seconds', 'time:duration'},
        ),
        NanosecondMetric(
            'total:time:wait',
            ('stats', 'Total'),
            {'metric:counter', 'unit:seconds', 'time:duration'},
        ),
    ],
    'sectors': [
        Metric(
            'sectors',
            ('stats', 'Count'),
            {'metric:counter'},
        ),
    ],
    'io_time': [
        MillisecondMetric(
            'time',
            ('stats', 'Count'),
            {'metric:counter', 'unit:seconds', 'time:duration'},
        ),
    ],
}
