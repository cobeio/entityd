"""DeclerativeEntity

This creates entities based on a description in a config file, which can be
used to describe how entities are related.
"""

import base64
import collections
import logging
import os
import os.path
import pathlib
import re

import yaml

import entityd.pm


log = logging.getLogger(__name__)


class ValidationError(Exception):
    """Error in validating data from entity declaration file."""
    pass


@entityd.pm.hookimpl
def entityd_plugin_registered(pluginmanager, name):
    """Called to register the plugin."""
    if name == 'entityd.declentity':
        gen = DeclerativeEntity()
        pluginmanager.register(gen, name='entityd.declentity.DeclerativeEntity')


@entityd.pm.hookimpl
def entityd_addoption(parser):
    """Add command line options to the argparse parser."""
    parser.add_argument(
        '--declentity-dir',
        default=pathlib.Path(__file__).parent,
        type=str,
        help=('Directory to scan for entity declaration files.'),
    )


RelDesc = collections.namedtuple('RelationDescription', ['type', 'attrs'])


class DeclerativeEntity:
    """Plugin to generate Declerative Entities."""
    # Private Attributes
    #
    # _host_ueid: The UEID of the host entity
    # _path: The path scanned for entity declaration files
    # _conf_attrs: A dictionary of lists, each being the terms used to describe
    #              entities which will be found
    # _deleted: A dictionary of sets, with the key being the name of an entity
    #           type, and the set being ueids of entities which are no longer
    #           being monitored.

    prefix = 'entityd.declentity:'

    def __init__(self):
        self._host_ueid = None
        self._path = None
        self.session = None
        self._conf_attrs = collections.defaultdict(list)
        self._deleted = collections.defaultdict(set)

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Configure the declerative entity creator with the dir path."""
        self._path = config.args.declentity_dir

    @entityd.pm.hookimpl(after='entityd.kvstore')
    def entityd_sessionstart(self, session):
        """Load previously known entity UEIDs."""
        self.session = session
        self._load_files()
        loaded_values = session.svc.kvstore.getmany(self.prefix)
        for key, ent_type in loaded_values.items():
            ueid = base64.b64decode(key.split(':', 1)[1])
            data = self._validate_conf(
                self._conf_attrs.get(ent_type, dict(type=ent_type)))
            expected = self._create_declerative_entity(data).ueid
            if ueid not in expected:
                self._deleted[ent_type].add(ueid)

    @entityd.pm.hookimpl(before='entityd.kvstore')
    def entityd_sessionfinish(self):
        """Called when the monitoring session ends to save UEIDs.

        Save the UEID and type of known entities in the kvstore to be restored
        when the session is next started.
        """
        self.session.svc.kvstore.deletemany(self.prefix)
        to_add = dict()
        for entity_type in self._conf_attrs:
            for entity_desc in self._conf_attrs[entity_type]:
                ueid = self._create_declerative_entity(entity_desc).ueid
                key = self.prefix + base64.b64encode(ueid).decode('ascii')
                to_add[key] = entity_type
        self.session.svc.kvstore.addmany(to_add)

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs):
        """Return an iterator of Monitored Entities.

        :param name: The name of the entity type to return.
        :param attrs: A dictionary of attributes to filter return entities on.
        """
        if name in self._deleted.keys():
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported'
                                  ' for attrs {}'.format(attrs))
            for ueid in self._deleted[name]:
                yield self._deleted_entity(name, ueid)
            self._deleted.pop(name)
        if name in self._conf_attrs.keys():
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported'
                                  ' for attrs {}'.format(attrs))
            for entity_desc in self._conf_attrs[name]:
                yield self._create_declerative_entity(entity_desc)

    @staticmethod
    def _deleted_entity(name, ueid):
        """Return a deleted entity with the type and ueid specified.

        :param name: The type of entity to return
        :param ueid: The ueid of the entity to return
        """
        # Not enough information to build the ueid, so set it explicitly
        entity = entityd.entityupdate.EntityUpdate(name, ueid=ueid)
        entity.delete()
        return entity

    def _load_files(self):
        """Load files, read the config data and add it to the conf_data dict.

        The folder path to search is provided by the entityd.config module
        Entity description files must end in .conf
        Each entity description must have a `type` to be valid.
        """
        if not self._path:
            return
        for dirpath, _, filenames in os.walk(self._path):
            for filename in filenames:
                if not filename.endswith('.conf'):
                    continue
                filepath = os.path.join(dirpath, filename)
                with open(filepath, 'r') as openfile:
                    try:
                        load_data = list(yaml.safe_load_all(openfile))
                    except yaml.scanner.ScannerError as err:
                        log.warning('Error loading file %s: %s',
                                    filepath, err)
                        continue
                for data in load_data:
                    if not isinstance(data, dict):
                        log.warning('Error loading file %s, one or more entity'
                                    'declarations not loaded.', filepath)
                        continue
                    data['filepath'] = filepath
                    try:
                        data = self._validate_conf(data)
                    except ValidationError as err:
                        log.warning('Error validating entity declaration '
                                    'in %s: %s', filepath, err)
                    else:
                        self._add_conf(data)

    @staticmethod
    def _validate_conf(data):
        """Validate the dictionary `data` as containing an entity description.

        :param data: A dictionary of entity configuration data to store.

        Returns a dictionary of the validated configuration data.

        Raises ValidationError if any of the elements cannot be validated.
        """
        if 'type' not in data.keys():
            raise ValidationError('No type field found in entity.')
        if '/' in data['type']:
            raise ValidationError("'/' not allowed in type field.")
        data.setdefault('filepath', '')
        data.setdefault('attrs', dict())
        data['children'] = DeclerativeEntity._validate_relations(
            data.get('children', []))
        data['parents'] = DeclerativeEntity._validate_relations(
            data.get('parents', []))

        if RelDesc('Host', {}) not in data['parents']:
            data['parents'].append(RelDesc('Host', {}))
        return data

    @staticmethod
    def _validate_relations(rel_list):
        """Validate a list of relations, converting them to RelDesc tuples.

        :param rel_list: A list of relations which will be validated

        Returns a list of RelDesc tuples matching the dictionaries in the
        original rel_list.

        Raises ValidationError if any of the relations cannot be validated.
        """
        validated = []
        for desc in rel_list:
            if isinstance(desc, RelDesc):
                validated.append(desc)
            elif isinstance(desc, dict):
                try:
                    relation = RelDesc(desc.pop('type'), desc)
                except KeyError:
                    raise ValidationError(
                        "'type' is required for relation definition")
                else:
                    validated.append(relation)
            else:
                raise ValidationError(
                    'Bad relation description, expected dictionary, got {}'
                    .format(type(desc).__name__))
        return validated

    def _add_conf(self, data):
        """Add the configuration given in `data` to the list of known data.

        :param data: A dictionary of pre-validated entity confiuration data.
        """
        self._conf_attrs[data['type']].append(data)
        try:
            self.session.config.addentity(
                data['type'], 'entityd.declentity.DeclerativeEntity')
        except KeyError:
            pass

    @property
    def host_ueid(self):
        """Property to get the host ueid, used in a few places"""
        if not self._host_ueid:
            results = self.session.pluginmanager.hooks.entityd_find_entity(
                name='Host', attrs=None)
            if results:
                host_me = next(iter(results[0]))
                self._host_ueid = host_me.ueid
        return self._host_ueid

    def _create_declerative_entity(self, config_properties):
        """Create a new declerative entity structure for the file.

        :param config_properties: A dictionary of properties to use when
                                  creating the entity.
        """
        # Parents and children are generators, not sets, to delay evaluation
        # allowing for successful recursive relationships.
        entity = entityd.EntityUpdate(config_properties.get('type'))
        entity.attrs.set('filepath',
                         config_properties['filepath'],
                         attrtype='id')
        entity.attrs.set('host', self.host_ueid, attrtype='id')
        for name, value in config_properties['attrs'].items():
            if isinstance(value, dict):
                attr_type = value.get('type', None)
                attr_val = value.get('value', None)
            else:
                attr_type = None
                attr_val = value
            entity.attrs.set(name, attr_val, attrtype=attr_type)

        # pylint: disable=protected-access
        entity.parents._relations = (
            entity.ueid for relation in config_properties['parents']
            for entity in self._find_entities(relation.type, relation.attrs)
        )

        entity.children._relations = (
            entity.ueid for relation in config_properties['children']
            for entity in self._find_entities(relation.type, relation.attrs)
        )

        return entity

    def _find_entities(self, entity_type, attrs):
        """Find entities of type entity_type matching the description in attrs.

        Returns a generator of all matching entities.

        :param entity_type: The type of the entity to find

        :param attrs: A dictionary of the attributes of the entity to find.
            The keys of the dictionary should be the attribute name.
            The values of the dictionary should be regular expressions which
                will be matched against the value of the attribute in the
                entities found.
            There can be any number of attributes to match, all of which must
                match successfully for the entitiy to be considered a match.
        """
        # Get the type, then ask pluginmanager for all entities of that type.
        # This returns a list of generators, need to iterate through all the
        # entities in all the generators in the list and test for matching
        # arguments, then return a generator of those matching entities.
        found_entities = self.session.pluginmanager.hooks.entityd_find_entity(
            name=entity_type, attrs=None)
        for entity_gen in found_entities:
            for entity in entity_gen:
                for attr_name, attr_val in attrs.items():
                    try:
                        re_term = entity.attrs.get(attr_name).value
                    except KeyError:
                        break
                    if not re.search(attr_val, re_term):
                        break
                else:
                    yield entity
