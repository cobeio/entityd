"""Helpers for Group entities."""

import entityd


def group(kind, id_):
    """Create a ``Group entity`` update.

    The group kind and identifier will be coerced to strings before
    attaching them to the entity update as attributes.

    :param kind: Kind/category of the group as a string.
    :param id_: Group identifier for the given kind/category.

    :returns: :class:`entityd.EntityUpdate` for the group.
    """
    group_ = entityd.EntityUpdate('Group')
    group_.label = str(id_)
    group_.attrs.set('kind', str(kind), traits={'entity:id'})
    group_.attrs.set('id', str(id_), traits={'entity:id'})
    return group_


def labels(mapping):
    """Create label groups from a mapping.

    For each pair in the mapping, the key is prefixed with `label:`
    and used as the *kind*. The corresponding value is used as the
    group ID.

    :returns: Iterator of ``Group`` entity updates.
    """
    for key in mapping:
        value = mapping[key]
        update = group('label:' + str(key), value)
        update.label = '{key} = {value}'.format(**locals())
        yield update
