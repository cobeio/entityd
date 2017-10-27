"""Helpers for Group entities."""

import entityd


def group(kind, id_):
    """Create a ``Group entity`` update."""
    group = entityd.EntityUpdate('Group')
    group.label = '{kind} = {id_}'.format(**locals())
    group.attrs.set('kind', kind, traits={'entity:id'})
    group.attrs.set('id', id_, traits={'entity:id'})
    return group


def labels(mapping):
    """Create label groups from a mapping.

    For each pair in the mapping, the key is prefixed with `label:`
    and used as the *kind*. The corresponding value is used as the
    group ID.

    :returns: Iterator of ``Group`` entity updates.
    """
    for key in mapping:
        value = mapping[key]
        yield group('label:' + str(key), value)
