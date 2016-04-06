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

import entityd.pm


log = logbook.Logger(__name__)
RFC_3339_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
_LOGGED_K8S_UNREACHABLE = False
ENTITIES_PROVIDED = {
    'Kubernetes:Container': 'generate_containers',
    'Kubernetes:Namespace': 'generate_namespaces',
    'Kubernetes:Pod': 'generate_pods',
}
Point = collections.namedtuple('Point', ('timestamp', 'data'))
Metric = collections.namedtuple(
    'Metric', ('name', 'path', 'traits', 'transform'))
CONTAINER_METRICS = [Metric(*specification) for specification in (
    (
        'cpu:total',
        ('cpu', 'usage', 'total'),
        {'metric:counter', 'unit:seconds', 'time:duration'},
        lambda ns: ns / (10 ** 9),
    ),
    (
        'cpu:user',
        ('cpu', 'usage', 'user'),
        {'metric:counter', 'unit:seconds', 'time:duration'},
        lambda ns: ns / (10 ** 9),
    ),
    (
        'cpu:system',
        ('cpu', 'usage', 'system'),
        {'metric:counter', 'unit:seconds', 'time:duration'},
        lambda ns: ns / (10 ** 9),
    ),
    (
        'cpu:load-average',
        ('cpu', 'usage', 'load_average'),
        {'metric:guage'},
        None,
    ),
    (
        'load:sleeping',
        ('load_stats', 'nr_sleeping'),
        {'metric:guage'},
        None,
    ),
    (
        'load:running',
        ('load_stats', 'nr_running'),
        {'metric:guage'},
        None,
    ),
    (
        'load:stopped',
        ('load_stats', 'nr_stopped'),
        {'metric:guage'},
        None,
    ),
    (
        'load:uninterruptible',
        ('load_stats', 'nr_uninterruptible'),
        {'metric:guage'},
        None,
    ),
    (
        'load:io-wait',
        ('load_stats', 'nr_io_wait'),
        {'metric:guage'},
        None,
    ),
    (
        'memory:usage',
        ('memory', 'usage'),
        {'metric:guage', 'unit:bytes'},
        None,
    ),
    (
        'memory:working-set',
        ('memory', 'working_set'),
        {'metric:guage', 'unit:bytes'},
        None,
    ),
    (
        'memory:fail-count',
        ('memory', 'failcnt'),
        {'metric:counter'},
        None,
    ),
    (
        'memory:page-fault',
        ('memory', 'container_data', 'pgfault'),
        {'metric:counter'},
        None,
    ),
    (
        'memory:page-fault:major',
        ('memory', 'container_data', 'pgmajfault'),
        {'metric:counter'},
        None,
    ),
    (
        'network:tcp:established',
        ('network', 'tcp', 'Established'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp:syn-sent',
        ('network', 'tcp', 'SynSent'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp:syn-recv',
        ('network', 'tcp', 'SynRecv'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp:fin-wait-1',
        ('network', 'tcp', 'FinWait1'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp:fin-wait-2',
        ('network', 'tcp', 'FinWait2'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp:time-wait',
        ('network', 'tcp', 'TimeWait'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp:close',
        ('network', 'tcp', 'Close'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp:close-wait',
        ('network', 'tcp', 'CloseWait'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp:last-ack',
        ('network', 'tcp', 'LastAck'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp:listen',
        ('network', 'tcp', 'Listen'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp:closing',
        ('network', 'tcp', 'Closing'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp6:established',
        ('network', 'tcp6', 'Established'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp6:syn-sent',
        ('network', 'tcp6', 'SynSent'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp6:syn-recv',
        ('network', 'tcp6', 'SynRecv'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp6:fin-wait-1',
        ('network', 'tcp6', 'FinWait1'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp6:fin-wait-2',
        ('network', 'tcp6', 'FinWait2'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp6:time-wait',
        ('network', 'tcp6', 'TimeWait'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp6:close',
        ('network', 'tcp6', 'Close'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp6:close-wait',
        ('network', 'tcp6', 'CloseWait'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp6:last-ack',
        ('network', 'tcp6', 'LastAck'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp6:listen',
        ('network', 'tcp6', 'Listen'),
        {'metric:gauge'},
        None,
    ),
    (
        'network:tcp6:closing',
        ('network', 'tcp6', 'Closing'),
        {'metric:gauge'},
        None,
    ),
)]


@entityd.pm.hookimpl
def entityd_configure(config):
    """Configure Kubernetes entities.

    This registers all the entities implemented by this module.
    """
    for entity_type in ENTITIES_PROVIDED:
        config.addentity(entity_type, __name__)


@entityd.pm.hookimpl
def entityd_find_entity(name, attrs=None, include_ondemand=False):  # pylint: disable=unused-argument
    """Find Kubernetes entities.

    :raises LookupError: if ``attrs`` is given.
    """
    if name in ENTITIES_PROVIDED:
        if attrs is not None:
            raise LookupError('Attribute based filtering not supported')
        return generate_updates(globals()[ENTITIES_PROVIDED[name]])


def generate_updates(generator_function):
    """Wrap an entity update generator function.

    This function wraps around any entity update generator and
    manages the :class:`kube.Cluster`'s life-time and creation
    of :class:`entityd.EntityUpdate`s.

    When the generator function is initially called it is
    passed a :class:`kube.Cluster`. Then it is continually sent
    :class:`entityd.EntityUpdate`s until the generator is exhausted.

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
            generator = generator_function(cluster)
            next(generator)
            while True:
                update = entityd.EntityUpdate(name)
                try:
                    generator.send(update)
                except StopIteration:
                    yield update
                    break
                else:
                    yield update
        except requests.ConnectionError:
            if not _LOGGED_K8S_UNREACHABLE:
                log.info('Kubernetes API server unreachable')
                _LOGGED_K8S_UNREACHABLE = True
        else:
            _LOGGED_K8S_UNREACHABLE = False


def apply_meta_update(meta, update):
    """Apply update attributes for a :class:`kube.ObjectMeta`.

    This sets attributes on the ``update`` that match the corresponding
    values on the given ``meta`` object. Each attribute name will be
    the same as on the meta object but prefixed by ``meta:``.

    The meta object's name and namespace (if set for the given object meta)
    become ``id``-typed attributes.

    :param kube.ObjectMeta meta: the meta object to set attributes for.
    :param entityd.EntityUpdate update: the update to apply the attributes to.
    """
    update.attrs.set('meta:name', meta.name, traits={'entity:id'})
    try:
        update.attrs.set('meta:namespace',
                         meta.namespace, traits={'entity:id'})
    except kube.StatusError:
        pass
    update.attrs.set('meta:version', meta.version)
    update.attrs.set(
        'meta:created',
        meta.created.strftime(RFC_3339_FORMAT),
        traits={'chrono:rfc3339'},
    )
    # TODO: Maybe convert to absolute URI
    update.attrs.set('meta:link', meta.link, traits={'uri'})
    update.attrs.set('meta:uid', meta.uid)
    # TODO: Labels


def generate_namespaces(cluster):
    """Generate updates for namespaces."""
    for namespace in cluster.namespaces:
        namespace_update(namespace, (yield))


def namespace_update(namespace, update):
    """Populate update with attributes for a container.

    This will apply metadata attributes as well as a ``phase`` attribute
    indicating the phase of the namespace.

    :param kube.Container container: the container to set attributes for.
    :param entityd.EntityUpdate update: the update to set the attributes on.
    """
    update.label = namespace.meta.name
    apply_meta_update(namespace.meta, update)
    update.attrs.set(
        'phase', namespace.phase.value, traits={'kubernetes:namespace-phase'})


def generate_pods(cluster):
    """Generate updates for pods.

    Pods will added as children of corresponding namespace entities.
    """
    namespaces = {}
    for namespace in generate_updates(generate_namespaces):
        namespaces[namespace.attrs.get('meta:name').value] = namespace
    for pod in cluster.pods:
        update = yield
        pod_update(pod, update)
        parent_namespace = namespaces.get(pod.meta.namespace)
        if parent_namespace:
            update.parents.add(parent_namespace)


def pod_update(pod, update):
    """Populate update with attributes for a pod.

    :param kube.Pod pod: the pod to set attributes for.
    :param entityd.EntityUpdate update: the update to set the attributes on.

    :returns: the ``update`` with additional attributes.
    """
    update.label = pod.meta.name
    apply_meta_update(pod.meta, update)
    update.attrs.set(
        'phase', pod.phase.value, traits={'kubernetes:pod-phase'})
    update.attrs.set(
        'start_time',
        pod.start_time.strftime(RFC_3339_FORMAT),
        traits={'chrono:rfc3339'},
    )
    update.attrs.set('ip', str(pod.ip),
                     traits={'ipaddr:v{}'.format(pod.ip.version)})
    for attribute in ('message', 'reason'):
        try:
            value = getattr(pod, attribute)
        except kube.StatusError:
            pass
        else:
            update.attrs.set(attribute, value)
    return update


def generate_containers(cluster):
    """Generate updates for containers.

    :returns: a generator of :class:`entityd.EntityUpdate`s.
    """
    for pod_update in generate_updates(generate_pods):  # pylint: disable=redefined-outer-name
        try:
            namespace = cluster.namespaces.fetch(
                pod_update.attrs.get('meta:namespace').value)
            pod = namespace.pods.fetch(
                pod_update.attrs.get('meta:name').value)
        except LookupError:
            pass
        else:
            for container in pod.containers:
                update = yield
                update.parents.add(pod_update)
                container_metrics(cluster, container, update)
                container_update(container, update)


def container_update(container, update):
    """Populate update with attributes for a container.

    :param kube.Container container: the container to set attributes for.
    :param entityd.EntityUpdate update: the update to set the attributes on.
    """
    update.label = container.name
    update.attrs.set('id', container.id, traits={'entity:id'})
    update.attrs.set('name', container.name, traits={'entity:id'})
    update.attrs.set('ready', container.ready)
    update.attrs.set('image:id', container.image.id)
    update.attrs.set('image:name', container.image.name)
    for state in ('running', 'waiting', 'terminated'):
        if getattr(container.state, state):
            update.attrs.set('state', state)
    if container.state.running or container.state.terminated:
        update.attrs.set(
            'state:started-at',
            container.state.started_at.strftime(RFC_3339_FORMAT),
            traits={'chrono:rfc3339'},
        )
    else:
        update.attrs.delete('state:started-at')
    if container.state.waiting or container.state.terminated:
        update.attrs.set('state:reason', container.state.reason)
    else:
        update.attrs.delete('state:reason')
    if container.state.terminated:
        update.attrs.set('state:exit-code', container.state.exit_code)
        update.attrs.set('state:signal', container.state.signal)
        update.attrs.set('state:message', container.state.message)
        update.attrs.set(
            'state:finished-at',
            container.state.finished_at.strftime(RFC_3339_FORMAT),
            traits={'chrono:rfc3339'},
        )
    else:
        for attribute in ('exit-code', 'signal', 'message', 'finished-at'):
            update.attrs.delete('state:' + attribute)


def select_nearest_point(when, points, threshold):
    differences = []
    for point in points:
        differences.append(
            (abs((when - point.timestamp).total_seconds()), point))
    differences.sort(key=lambda d: d[0])
    difference, point = differences[0]
    if difference > threshold:
        raise ValueError('No metric point within {} seconds'.format(threshold))
    return point


def cadvisor_to_points(raw_points):
    points = []
    for point in raw_points:
        date_and_time, us_and_offset = point['timestamp'].split('.')
        for offset_separator in ('Z', '+', '-'):
            if offset_separator in us_and_offset:
                us, raw_offset = us_and_offset.split(offset_separator)
                us = us[:6]
                if raw_offset:
                    hours, minutes = raw_offset.split(':', 1)
                    offset = datetime.timedelta(
                        hours=int(hours), minutes=int(minutes))
                else:
                    offset = datetime.timedelta()
                break
        normalised_datetime = date_and_time + '.' + us
        timestamp = datetime.datetime.strptime(
            normalised_datetime, '%Y-%m-%dT%H:%M:%S.%f') + offset
        points.append(Point(timestamp, point))
    return points


def point_to_attributes(point, update):
    delete = []
    for metric in CONTAINER_METRICS:
        value = point.data
        for step in metric.path:
            try:
                value = value[step]
            except KeyError:
                delete.append(metric.name)
                log.debug(
                    'Could not determine value for metric {}'.format(metric))
                break
        if metric.name in delete:
            update.attrs.delete(metric.name)
        else:
            if metric.transform:
                value = metric.transform(value)
            update.attrs.set(metric.name, value, metric.traits)


def container_metrics(cluster, container, update):
    now = datetime.datetime.utcnow()
    for node in cluster.nodes:
        try:
            response = cluster.proxy.get(
                'proxy/nodes', node.meta.name + ':4194',
                'api/v2.0/stats', container.id, type='docker')
        except kube.APIError as exc:
            pass
        else:
            points = cadvisor_to_points(response['/' + container.id])
            try:
                point = select_nearest_point(now, points, 5)
            except ValueError as exc:
                log.warning(
                    '{} for container with ID {}'.format(exc, container.id))
            else:
                point_to_attributes(point, update)
            return
    log.warning(
        'Could not find node for container with ID {}'.format(container.id))
