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
    'Kubernetes:Pod': 'generate_pods',
    # 'Kubernetes:Container': 'generate_containers',
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

    The meta object's name and namespace become ``id``-typed attributes.

    :param kube.ObjectMeta meta: the meta object to set attributes for.
    :param entityd.EntityUpdate update: the update to apply the attributes to.
    """
    update.attrs.set('meta:name', meta.name, attrtype='id')
    update.attrs.set('meta:namespace', meta.namespace, attrtype='id')
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
    update.attrs.set('ip', str(pod.ip), attrtype='ip:v4')
    for attribute in ('message', 'reason'):
        try:
            value = getattr(pod, attribute)
        except kube.StatusError:
            pass
        else:
            update.attrs.set(attribute, value)
    return update


# def generate_containers(cluster):
#     """Generate updates for containers.
#
#     :returns: a generator of :class:`entityd.EntityUpdate`s.
#     """
