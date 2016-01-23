"""Plugin providing entities for Kubernetes.

This module implements all the entities for various Kubernetes
components. Each entity type is implemented as a generator function.
A single ``entityd_find_entity`` hook implementation takes responsibility
for dispatching to the correct generator function.
"""

import entityd.pm

import kube


RFC_3339_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
ENTITIES_PROVIDED = {
    'Kubernetes:Container': 'generate_containers',
    'Kubernetes:Namespace': 'generate_namespaces',
    'Kubernetes:Pod': 'generate_pods',
}


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
    name = {value: key for key, value
            in ENTITIES_PROVIDED.items()}[generator_function.__name__]
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
    update.attrs.set('meta:name', meta.name, attrtype='id')
    try:
        update.attrs.set('meta:namespace', meta.namespace, attrtype='id')
    except kube.StatusError:
        pass
    update.attrs.set('meta:version', meta.version)
    update.attrs.set(
        'meta:created',
        meta.created.strftime(RFC_3339_FORMAT),
        attrtype='chrono:rfc3339',
    )
    # TODO: Maybe convert to absolute URI
    update.attrs.set('meta:link', meta.link, attrtype='uri')
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
        'phase', namespace.phase.value, attrtype='kubernetes:namespace-phase')


def generate_pods(cluster):
    """Generate updates for pods.

    :returns: a generator of :class:`entityd.EntityUpdate`s.
    """
    for pod in cluster.pods:
        pod_update(pod, (yield))


def pod_update(pod, update):
    """Populate update with attributes for a pod.

    :param kube.Pod pod: the pod to set attributes for.
    :param entityd.EntityUpdate update: the update to set the attributes on.

    :returns: the ``update`` with additional attributes.
    """
    update.label = pod.meta.name
    apply_meta_update(pod.meta, update)
    update.attrs.set(
        'phase', pod.phase.value, attrtype='kubernetes:pod-phase')
    update.attrs.set(
        'start_time',
        pod.start_time.strftime(RFC_3339_FORMAT),
        attrtype='chrono:rfc3339',
    )
    update.attrs.set('ip', str(pod.ip),
                     attrtype='ip:v{}'.format(pod.ip.version))
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
    for pod_update in generate_updates(generate_pods):
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
            container.state.started_at.strftime(RFC_3339_FORMAT),
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
            container.state.finished_at.strftime(RFC_3339_FORMAT),
            attrtype='chrono:rfc3339',
        )
    else:
        for attribute in ('exit-code', 'signal', 'message', 'finished-at'):
            update.attrs.delete('state:' + attribute)
