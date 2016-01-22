"""Plugin providing entities for Kubernetes.

This module implements all the entities for various Kubernetes
components. Each entity type is implemented as a generator function.
A single ``entityd_find_entity`` hook implementation takes responsibility
for dispatching to the correct generator function.
"""

import entityd.pm

import kube


_RFC_3339_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
_ENTITIES_PROVIDED = {  # Entity type : update generator function name
    'Kubernetes:' + type_:
    '_generate_' + function for type_, function in [
        ('Pod', 'pods'),
        ('Container', 'containers'),
    ]
}


@entityd.pm.hookimpl
def entityd_configure(config):
    """Configure Kubernetes entities.

    This registers all the entities implemented by this module.
    """
    for entity_type in _ENTITIES_PROVIDED:
        config.addentity(entity_type, __name__)


@entityd.pm.hookimpl
def entityd_find_entity(name, attrs=None, include_ondemand=False):  # pylint: disable=unused-argument
    """Find Kubernetes entities.

    :raises LookupError: if ``attrs`` is given.
    """
    if name in _ENTITIES_PROVIDED:
        if attrs is not None:
            raise LookupError('Attribute based filtering not supported')
        return _generate_updates(globals()[_ENTITIES_PROVIDED[name]])


def _generate_updates(generator_function):
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
        not registered in :data:`_ENTITIES_PROVIDED`.

    :returns: a generator of the updates returned by ``generator_function``.
    """
    name = {value: key for key, value
            in _ENTITIES_PROVIDED.items()}[generator_function.__name__]
    with kube.Cluster() as cluster:
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


def _apply_meta_update(meta, update):
    """Apply update attributes for a :class:`kube.ObjectMeta`.

    This sets attributes on the ``update`` that match the corresponding
    values on the given ``meta`` object. Each attribute name will be
    the same as on the meta object but prefixed by ``meta:``.

    The meta object's name and namespace become ``id``-typed attributes.

    :param kube.ObjectMeta meta: the meta object to set attributes for.
    :param entityd.EntityUpdate update: the update to apply the attributes to.
    """
    update.attrs.set('meta:name', meta.name, attrtype='id')
    update.attrs.set('meta:namespace', meta.namespace, attrtype='id')
    update.attrs.set('meta:version', meta.version)
    update.attrs.set(
        'meta:created',
        meta.created.strftime(_RFC_3339_FORMAT),
        attrtype='chrono:rfc3339',
    )
    # TODO: Maybe convert to absolute URI
    update.attrs.set('meta:link', meta.link, attrtype='uri')
    update.attrs.set('meta:uid', meta.uid)
    # TODO: Labels


def _generate_pods(cluster):
    """Generate updates for pods.

    :returns: a generator of :class:`entityd.EntityUpdate`s.
    """
    for pod in cluster.pods:
        _pod_update(pod, (yield))


def _pod_update(pod, update):
    """Populate update with attributes for a pod.

    :param kube.Pod pod: the pod to set attributes for.
    :param entityd.EntityUpdate update: the update to set the attributes on.

    :returns: the ``update`` with additional attributes.
    """
    update.label = pod.meta.name
    _apply_meta_update(pod.meta, update)
    update.attrs.set(
        'phase', pod.phase.value, attrtype='kubernetes:pod-phase')
    update.attrs.set(
        'start_time',
        pod.start_time.strftime(_RFC_3339_FORMAT),
        attrtype='chrono:rfc3339',
    )
    update.attrs.set('ip', str(pod.ip), attrtype='ip:v4')
    for attribute in ('message', 'reason'):
        try:
            value = getattr(pod, attribute)
        except kube.StatusError:
            pass
        else:
            update.attrs.set(attribute, value)
    return update


def _generate_containers(cluster):
    """Generate updates for containers.

    :returns: a generator of :class:`entityd.EntityUpdate`s.
    """
    for pod_update in _generate_updates(_generate_pods):
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
                container_update(container, update)


def container_update(container, update):
    """Populate update with attributes for a container.

    :param kube.Container container: the container to set attributes for.
    :param entityd.EntityUpdate update: the update to set the attributes on.
    """
    update.label = container.name
    update.attrs.set('id', container.id, attrtype='id')
    update.attrs.set('name', container.name, attrtype='id')
    update.attrs.set('ready', container.ready)
    update.attrs.set('image:id', container.image.id)
    update.attrs.set('image:name', container.image.name)
    for state in ('running', 'waiting', 'terminated'):
        if getattr(container.state, state):
            update.attrs.set('state', state)
    if container.state.running or container.state.terminated:
        update.attrs.set(
            'state:started-at',
            container.state.started_at.strftime(_RFC_3339_FORMAT),
            attrtype='chrono:rfc3339',
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
            container.state.finished_at.strftime(_RFC_3339_FORMAT),
            attrtype='chrono:rfc3339',
        )
    else:
        for attribute in ('exit-code', 'signal', 'message', 'finished-at'):
            update.attrs.delete('state:' + attribute)
