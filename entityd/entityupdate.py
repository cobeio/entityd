"""An EntityUpdate contains the updated information for an Entity"""

import collections
import hashlib
import time


class EntityUpdate:
    """An update for this Entity.

    Attributes and relations can be added via the methods on
    EntityUpdate.attrs, EntityUpdate.parents and EntityUpdate.children.

    """
    # pylint: disable=too-many-instance-attributes
    def __init__(self, metype, ueid=None):
        self.metype = metype
        self.label = None
        self.timestamp = time.time()
        self.attrs = UpdateAttributes()
        self.parents = UpdateRelations()
        self.children = UpdateRelations()
        self.deleted = False
        self._ueid = ueid

    def delete(self):
        """Mark this EntityUpdate as deleted."""
        self.deleted = True

    @property
    def ueid(self):
        """Generate and return a UEID based on set attributes.

        Only attributes with the trait 'entity:id' are used to create the ueid.

        All identifying attributes *must* be set before accessing the
        ueid, otherwise it will be incorrect.

        """
        if self._ueid:
            return self._ueid
        attr_parts = ['{name}={value}'.format(name=attr.name, value=attr.value)
                      for attr in self.attrs
                      if 'entity:id' in attr.traits]
        attr_parts.sort()
        strval = self.metype + '|' + '|'.join(attr_parts)
        hash_ = hashlib.sha1(strval.encode('utf-8'))
        return hash_.digest()


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
        if not isinstance(value, (int, bool, float, str, bytes)):
            value = str(value)
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
        """Get all deleted attribute names."""
        return self._deleted_attrs


class UpdateRelations:
    """A set of UEIDs; either parent or child relations."""

    def __init__(self):
        self._relations = set()

    def __iter__(self):
        return self._relations.__iter__()

    def add(self, entity):
        """Add entity to the relations for this update.

        :param entity: Either a UEID or an EntityUpdate object.

        """
        if isinstance(entity, EntityUpdate):
            ueid = entity.ueid
        else:
            ueid = entity
        self._relations.add(ueid)
