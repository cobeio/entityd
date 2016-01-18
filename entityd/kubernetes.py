"""Plugin providing entities for Kubernetes.

This module implements all the entities for various Kubernetes
components. Each entity type is implemented as a generator function.
A single ``entityd_find_entity`` hook implementation takes responsibility
for dispatching to the correct generator function.
"""

import entityd.pm

import kube


_ENTITIES_PROVIDED = {  # Entity type : update generator function name
    'Kubernetes:' + type_:
    '_generate_' + function for type_, function in [
        ('Pod', 'pods'),
        # ('Container', 'containers'),
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
def entityd_find_entity(name, attrs, include_ondemand):
    """Find Kubernetes entities.

    :raises LookupError: if ``attrs`` is given.
    """
    if name in _ENTITIES_PROVIDED:
        if attrs is not None:
            raise LookupError('Attribute based filtering not supported')
        return _generate_updates(name, globals()[_ENTITIES_PROVIDED[name]])


def _generate_updates(name, generator_function):
    """Wrap an entity update generator function.

    This function wraps around any entity update generator and
    manages the :class:`kube.Cluster`'s life-time and creation
    of :class:`entityd.EntityUpdate`s.

    When the generator function is initially called it is
    passed a :class:`kube.Cluster`. Then it is continually sent
    :class:`entityd.EntityUpdate`s until the generator is exhausted.

    :param str name: the entity type name that the function generates.
    :param generator_function: a generator function that yields
        :class:`entityd.EntityUpdate`s.

    :returns: a generator of the updates returned by ``generator_function``.
    """
    with kube.Cluster() as cluster:
        generator = generator_function(cluster)
        next(generator)
        while True:
            update = entityd.EntityUpdate(name)
            try:
                populated_update = generator.send(update)
            except StopIteration:
                break
            else:
                yield populated_update


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
    update.attrs.set('meta:created',
                     str(meta.created), attrtype='chrono:rfc3339')
    # TODO: Maybe convert to absolute URI
    update.attrs.set('meta:link', meta.link, attrtype='uri')
    update.attrs.set('meta:uid', meta.uid)
    # TODO: Labels


def _generate_pods(cluster):
    """Generate updates for pods.

    :returns: a generator of :class:`entityd.EntityUpdate`s.
    """
    update = yield
    for pod in cluster.pods:
        update = yield _pod_update(pod, update)


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
        'start_time', str(pod.start_time), attrtype='chrono:rfc3339')
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
