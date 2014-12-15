"""An EntityUpdate contains the updated information for an Entity"""

import hashlib
import time


class EntityUpdate:
    """An update for this Entity.

    Attributes and relations can be added via the methods on
    EntityUpdate.attrs, EntityUpdate.parents and EntityUpdate.children.
    """
    def __init__(self, metype):
        self.metype = metype
        self.timestamp = time.time()
        self.attrs = UpdateAttributes()
        self.parents = UpdateRelations()
        self.children = UpdateRelations()
        self.deleted = False

    def delete(self):
        """Mark this EntityUpdate as deleted"""
        self.deleted = True

    @property
    def ueid(self):
        """Generate and return a UEID based on set attributes.

        Attributes with type 'id' are used to create the ueid.

        """
        attr_parts = ['{name}={value}'.format(name=key, value=attr['value'])
                      for key, attr in self.attrs.items()
                      if attr.get('type') == 'id']
        attr_parts.sort()
        strval = self.metype + '|' + '|'.join(attr_parts)
        hash_ = hashlib.sha1(strval.encode('utf-8'))
        return hash_.digest()


class UpdateAttributes:
    """Store attributes for an EntityUpdate.

    Updates are stored as a map with a required 'value' field and
    optional 'type' and 'deleted' fields.

    """
    def __init__(self):
        self._attrs = {}

    def items(self):
        """Return (key, mapping) pairs for each known item"""
        return self._attrs.items()

    def set(self, key, value, attrtype=None):
        """Set an attributes value and type.

        :param key: The attribute to set.
        :param value: The value of the attribute.
        :param attrtype: Optional type for the attribute.
        """
        self._attrs[key] = {'value': value}
        if attrtype:
            self._attrs[key]['type'] = attrtype

    def delete(self, key):
        """Mark the given attribute as deleted."""
        self._attrs[key]['deleted'] = True

    def getvalue(self, key):
        """Get the value embedded in key"""
        return self._attrs[key]['value']


class UpdateRelations:
    """A set of UEIDs; either parent or child relations."""
    def __init__(self):
        self._relations = set()

    def add(self, entity):
        """Add entity to the relations for this update.

        :param entity: Either a UEID or an EntityUpdate object.

        """
        if isinstance(entity, EntityUpdate):
            ueid = entity.ueid
        else:
            ueid = entity
        self._relations.add(ueid)
