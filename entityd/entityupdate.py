"""An EntityUpdate contains the updated information for an Entity"""

import collections
import time

import cobe


class EntityUpdate:
    """An update for this Entity.

    Attributes and relations can be added via the methods on
    EntityUpdate.attrs, EntityUpdate.parents and EntityUpdate.children.

    :param ueid: The explicit UEID for the update. This will be converted
        to a :class:`cobe.UEID` if given.
    :type ueid: str or cobe.UEID

    :raises cobe.UEIDError: If the given UEID is the wrong length or
        contains invalid characters or otherwise cannot be converted to
        a valid :class:`cobe.UEID` instance.
    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, metype, ueid=None, ttl=120):
        self.metype = metype
        self.label = None
        self.timestamp = time.time()
        self.ttl = ttl
        self.attrs = UpdateAttributes()
        self.parents = UpdateRelations()
        self.children = UpdateRelations()
        self.exists = True
        if ueid:
            self._ueid = cobe.UEID(ueid)
        else:
            self._ueid = None

    def set_not_exists(self):
        """Mark this EntityUpdate as non existent."""
        self.exists = False

    @property
    def ueid(self):
        """Generate and return a UEID based on set attributes.

        Only attributes with the trait 'entity:id' are used to create
        the ueid.

        All identifying attributes *must* be set before accessing the
        ueid, otherwise it will be incorrect.

        If a UEID was set explicitly when constructing the update then
        that UEID will be returned instead of generating a new one.

        :raises cobe.UEIDError: If a UEID cannot be generated for the
            update. For example, if one of the identifying attributes is
            not of a valid type.

        :returns: A :class:`cobe.UEID` object representing the identifying
            attributes of the update.
        """
        if self._ueid:
            return self._ueid
        update = cobe.Update(self.metype)
        for attribute in self.attrs:
            update.attributes[attribute.name].set(attribute.value)
            update.attributes[attribute.name].traits.update(attribute.traits)
        return update.ueid()

    @ueid.setter
    def ueid(self, ueid):
        """Explicitly set the update's UEID.

        The given UEID will be converted to a :class:`cobe.UEID`.

        :raises cobe.UEIDError: If the given UEID is the wrong length or
            contains invalid characters or otherwise cannot be converted to
            a valid :class:`cobe.UEID` instance.
        """
        self._ueid = cobe.UEID(ueid)


UpdateAttr = collections.namedtuple('UpdateAttr', ['name', 'value', 'traits'])


class UpdateAttributes:
    """Store attributes for an EntityUpdate.

    Updates are stored as a map with a required 'value' field and
    optional 'type' and 'deleted' fields.

    """

    def __init__(self):
        self._attrs = {}
        self._deleted_attrs = set()

    def __iter__(self):
        return iter(self._attrs.values())

    def set(self, name, value, traits=None):
        """Set an attributes value and type.

        :param name: The attribute to set.
        :param value: The value of the attribute.
        :param traits: Optional set of traits for the attribute.

        """
        if traits is None:
            traits = set()
        else:
            traits = set(traits)
        self._attrs[name] = UpdateAttr(name, value, traits)

    def get(self, name):
        """Get the UpdateAttr for this name."""
        return self._attrs[name]

    def delete(self, name):
        """Mark the named attribute as deleted."""
        self._deleted_attrs.add(name)
        try:
            del self._attrs[name]
        except KeyError:
            pass

    def deleted(self):
        """Get all deleted attribute names.

        :returns: A copy of a set of all the attribute names
            marked for deletion.
        """
        return set(self._deleted_attrs)

    def clear(self, name):
        """Clear an attribute from the update by name.

        This drops the given attribute from the collection of attributes,
        whether it's been set or deleted.
        """
        try:
            del self._attrs[name]
        except KeyError:
            pass
        self._deleted_attrs.discard(name)


class UpdateRelations:
    """A set of UEIDs; either parent or child relations."""

    def __init__(self):
        self._relations = set()

    def __iter__(self):
        return self._relations.__iter__()

    def __len__(self):
        return len(self._relations)

    def add(self, entity):
        """Add entity to the relations for this update.

        :param entity: Either a UEID or an EntityUpdate object.
        :type entity: cobe.UEID or entityd.EntityUpdate

        :raises ValueError: If not given a valid UEID.
        """
        if isinstance(entity, EntityUpdate):
            ueid = entity.ueid
        else:
            ueid = entity
        if not isinstance(ueid, cobe.UEID):
            raise ValueError('Can only add UEID or EntityUpdate '
                             'as relations but got {!r}'.format(type(ueid)))
        self._relations.add(ueid)

    def discard(self, entity):
        """Discard an entity from the relations of this update.

        :param entity: Either a UEID or and EntityUpdate object.
        :type entity: cobe.UEID or entityd.EntityUpdate

        :raises ValueError: If not given a valid UEID.
        """
        if isinstance(entity, EntityUpdate):
            ueid = entity.ueid
        else:
            ueid = entity
        if not isinstance(ueid, cobe.UEID):
            raise ValueError('Can only delete UEID or EntityUpdate '
                             'as relations but got {!r}'.format(type(ueid)))
        self._relations.discard(ueid)
