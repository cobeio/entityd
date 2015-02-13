"""DeclerativeEntity

This creates entities based on a description in a config file, which can be
used to proscribe how entities are related.
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
        help=('File path to .conf files used to define DeclerativeEntities.'),
    )


class DeclerativeEntity:
    """Plugin to generate Declerative Entities."""

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
        if hasattr(config.args, 'declentity_dir'):
            self._path = config.args.declentity_dir

    @entityd.pm.hookimpl(after='entityd.kvstore')
    def entityd_sessionstart(self, session):
        """Load previously known entity UEIDs."""
        self.session = session
        self._load_files()
        loaded_values = session.svc.kvstore.getmany(self.prefix)
        for key, ent_type in loaded_values.items():
            ueid = base64.b64decode(key.split(':', 1)[1])
            expected = self._create_declerative_entity(
                self._conf_attrs.get(ent_type, dict(type=ent_type))).ueid
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
                        log.warning("Error loading file %s: %s",
                                    filepath, err)
                        continue
                for data in load_data:
                    if not isinstance(data, dict):
                        log.warning("Error loading file %s", filepath)
                        continue
                    data['filepath'] = filepath
                    self._add_conf(data)


    def _add_conf(self, data):
        """Add entity config data to the conf_data dictionary
        """
        if 'type' not in data.keys():
            log.warning("No type field found in file %s", data.get('filepath'))
            return
        if '/' in data['type']:
            log.warning("Invalid entity specification in file %s, "
                        "'\\' not allowed.", data.get('filepath'))
            return
        if 'attrs' not in data.keys():
            data['attrs'] = dict()
        if 'parents' not in data.keys():
            data['parents'] = list()
        if {'type': 'Host'} not in data['parents']:
            data['parents'].append({'type': 'Host'})
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

    def _create_declerative_entity(self, conf_attrs):
        """Create a new declerative entity structure for the file.

        :param conf_attrs: A dictionary of attributes to use when creating the
                           entity.
        """
        # Parents and children are generators, not sets, to delay evaluation
        # allowing for successful recursive relationships.
        entity = entityd.EntityUpdate(conf_attrs.get('type'))
        entity.attrs.set('filepath',
                         conf_attrs.get('filepath', ''),
                         attrtype='id')
        entity.attrs.set('host', self.host_ueid, attrtype='id')
        for name, value in conf_attrs.get('attrs', dict()).items():
            if isinstance(value, dict):
                attr_type = value.get('type', None)
                attr_val = value.get('value', None)
            else:
                attr_type = None
                attr_val = value
            entity.attrs.set(name, attr_val, attrtype=attr_type)

        # pylint: disable=protected-access
        entity.children._relations = (
            entity.ueid for entity in self._find_entities(
                conf_attrs.get('children', list())))

        entity.parents._relations = (
            entity.ueid for entity in self._find_entities(
                conf_attrs.get('parents', list())))
        return entity

    def _find_entities(self, args):
        """Find entities based on the descriptions given in `args`.

        Returns a generator of all matching entities.

        :param args: A list of dictionaries describing the entities to find.
            Each dictionary must have the key `type` whose value is the type
                of entity to find.
            All other keys in the dictionary should be attribute names.
            The value of all other keys should be regular expressions which
                will be matched against the value of the attribute in the
                entities found.
            There can be any number of attributes to match, all of which must
                match successfully for the entitiy to be considered a match.
            e.g.: [{type: <type>, <attr_name>: <attr_val>}]
        """
        # Get the type, then ask pluginmanager for all entities of that type.
        # This returns a list of generators, need to iterate through all the
        # entities in all the generators in the list and test for matching
        # arguments, then return a generator of those matching entities.
        for entity_description in args:
            if 'type' not in entity_description.keys():
                log.warning("'type' required to find entity, got %s",
                            entity_description)
                continue
            entities = self.session.pluginmanager.hooks.entityd_find_entity(
                name=entity_description['type'], attrs=None)
            for entity_gen in entities:
                for entity in entity_gen:
                    for attr_name, attr_val in entity_description.items():
                        if attr_name == 'type':
                            continue
                        try:
                            re_term = entity.attrs.get(attr_name).value
                        except KeyError:
                            break
                        if not re.search(attr_val, re_term):
                            break
                    else:
                        yield entity
