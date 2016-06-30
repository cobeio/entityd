"""Declarative Entity Plugin.

This creates entities based on a description in a config file, which can be
used to describe how entities are related.

Entity declaration files are written in yaml, and must have the suffix
".entity". Each file can contain one or more entity declaration, separated by
"---". Each entity must have a `type` and can also contain static attributes
specified under `attrs`, and children and parents which are specified by their
type and any regular expression matches to find the correct entity.

Attributes can be either given as a simple "name: value" pair, in which case
they will have no type, or as a dictionary also specifying the type. If
multiple entities of the same type are given in the same file then they must
also contain at least one attribute of type `id` with differing values.

Relations are given as a list of dictionaries, each dictionary corresponding
to different entity parameters. The relation must specify the `type` of the
entity to find, and can also include any number of additional attributes to
match on, where the key is the attribute name, and the value is a regular
expression to match the attribute value. All entities which match the terms
given will be added as relations, so specifying an relation with just a `type`
will add all entities of that type as relations.

Example entity declaration file:

.. yaml::

    type: EntityType
    attrs:
        owner: admin@default
        ident:
            value: 1
            type: id
    children:
        - type: Process
          command: [Cc]ommand
        - type: File
          path: /path/to/file
    parents:
        - type: Host
"""

import collections
import pathlib
import re

import act
import logbook
import yaml

import entityd.pm


log = logbook.Logger(__name__)


class ValidationError(Exception):
    """Error in validating data from entity declaration file."""
    pass


RelDesc = collections.namedtuple('RelationDescription', ['type', 'attrs'])


class DeclarativeEntity:
    """Plugin to generate Declarative Entities."""
    # Private Attributes
    #
    # _host_ueid: The UEID of the host entity
    # _path: The path scanned for entity declaration files
    # _conf_attrs: A dictionary of lists, the key is the type of the entities
    #              and the list contains DeclCfg objects describing how to
    #              build an entity.

    prefix = 'entityd.declentity:'

    def __init__(self):
        self._host_ueid = None
        self._path = act.fsloc.sysconfdir.joinpath('entity_declarations')
        self.session = None
        self._conf_attrs = collections.defaultdict(list)
        self._files = {}

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_addoption(parser):
        """Add command line options to the argparse parser."""
        parser.add_argument(
            '--declentity-dir',
            default=act.fsloc.sysconfdir.joinpath('entity_declarations'),
            type=pathlib.Path,
            help=('Directory to scan for entity declaration files.'),
        )

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Configure the declarative entity creator with the dir path."""
        self._path = config.args.declentity_dir

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        """Read config files and load declarations.

        This reads all config files and loads the declarations into the
        self._conf_attrs dictionary.
        """
        self.session = session
        self._update_entities()

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs, include_ondemand=False):  # pylint: disable=unused-argument
        """Return an iterator of Monitored Entities.

        :param name: The name of the entity type to return.
        :param attrs: A dictionary of attributes to filter return entities on.
        """
        self._update_entities()
        if name in self._conf_attrs.keys():
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported'
                                  ' for attrs {}'.format(attrs))
            for entity_desc in self._conf_attrs[name]:
                yield self._create_declarative_entity(entity_desc)

    def _update_entities(self):
        """Load files, read the config data and add it to self._conf_attrs.

        The folder path to search is provided by the entityd.core.Config class
        Entity description files must end in .entity
        Each entity description must have a `type` to be valid.
        """
        loaded = []
        changed_files = []
        for filepath in self._path.rglob('*.entity'):
            if filepath.stat().st_mtime != self._files.get(filepath, 0):
                log.debug("Loading file {}", filepath)
                loaded.extend([conf for conf in self._load_file(filepath)])
                changed_files.append(filepath)

        for ent_type in list(self._conf_attrs.keys()):
            for ent_desc in self._conf_attrs[ent_type][::-1]:
                if not ent_desc.filepath.exists():
                    self._remove_conf(ent_desc)
                if (ent_desc not in loaded
                        and ent_desc.filepath in changed_files):
                    self._remove_conf(ent_desc)

        for conf in loaded:
            self._add_conf(conf)

    def _load_file(self, filepath):
        """Load an entity declaration file and return DeclCfg objects.

        :param filepath: The path to the entity declaration file to open.

        :return: A generator of DeclCfg objects for each valid entity
            declaration in the file
        """
        try:
            with filepath.open('r') as openfile:
                try:
                    load_data = list(yaml.safe_load_all(openfile))
                except yaml.scanner.ScannerError as err:
                    log.warning('Could not load file {}: {}',
                                filepath, err)
                    return
        except IOError as err:
            log.warning("Could not open file {}: {}", filepath, err)
            return
        self._files[filepath] = filepath.stat().st_mtime
        for data in load_data:
            if not isinstance(data, dict):
                log.warning('Error loading file {}, one or more entity'
                            'declarations not loaded.', filepath)
                continue
            data['filepath'] = filepath
            try:
                data = DeclCfg(data)
            except ValidationError as err:
                log.warning('Ignoring invalid entity declaration '
                            'in {}: {}', filepath, err)
            else:
                yield data

    def _add_conf(self, data):
        """Add the configuration given in `data` to the list of known data.

        :param data: A dictionary of pre-validated entity confiuration data.
        """
        self._conf_attrs[data.type].append(data)
        try:
            self.session.config.addentity(
                data.type, 'entityd.declentity.DeclarativeEntity')
        except KeyError:
            pass

    def _remove_conf(self, data):
        """Removed the description `data` from the known descriptions

        Also removes that type key if appropriate and unregisters the type:
        """

        self._conf_attrs[data.type].remove(data)
        if not self._conf_attrs[data.type]:
            del self._conf_attrs[data.type]
            self.session.config.removeentity(
                data.type, 'entityd.declentity.DeclarativeEntity')

    @property
    def host_ueid(self):  # pragma: no cover
        """Property to get the host ueid, used in a few places.

        :raises LookupError: If a host UEID cannot be found.

        :returns: A :class:`cobe.UEID` for the  host.
        """
        if not self._host_ueid:
            results = self.session.pluginmanager.hooks.entityd_find_entity(
                name='Host', attrs=None)
            if results:
                host_me = next(iter(results[0]))
                self._host_ueid = host_me.ueid
        if not self._host_ueid:
            raise LookupError('Could not find the host UEID')
        return self._host_ueid

    def _create_declarative_entity(self, config_properties):
        """Create a new declarative entity structure for the file.

        :param config_properties: A dictionary of properties to use when
                                  creating the entity.
        """
        # Parents and children are generators, not sets, to delay evaluation
        # allowing for successful recursive relationships.
        entity = entityd.EntityUpdate(config_properties.type)
        entity.label = config_properties.label
        entity.attrs.set('filepath',
                         str(config_properties.filepath),
                         traits={'entity:id'})
        entity.attrs.set('hostueid', str(self.host_ueid),
                         traits={'entity:id', 'entity:ueid'})
        for name, value in config_properties.attrs.items():
            entity.attrs.set(name, value.value, traits=value.traits)

        # pylint: disable=protected-access
        entity.parents._relations = (
            entity.ueid for relation in config_properties.parents
            for entity in self._find_entities(relation.type, relation.attrs)
        )

        entity.children._relations = (
            entity.ueid for relation in config_properties.children
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


class DeclCfg:
    """A declarative entity configuration class

    This class holds the declarative entity data which is used to build a
    declarative entity.

    :param data: A dictionary of data used to create the declaration class.

    Raises ValidationError if any of the elements cannot be validated.

    Attributes:

    :attr type: The type of the entity. Required, cannot contain '/'
       characters.
    :attr label: The label for the entity. A short human-readable string for
                displaying the entity.
    :attr filepath: The path of the file the declaration was read from.
    :attr attrs: A dictionary of static attributes, the keys of the dictionary
                 are the attribute names, the values are instances of
                 entityd.entityupdate.UpdateAttr named tuples containing the
                 name, value and type of the attribute.
    :attr children: A list of RelDesc named tuples which are used to find the
                    ueids of the children of the entity.
    :attr parents: A list of RelDesc named tuples which are used to find the
                   ueids of the parents of the entity.

    """

    def __init__(self, data):
        self._type = None
        try:
            self.type = data['type']
        except KeyError:
            raise ValidationError('No type field found in entity.')
        try:
            self.label = data['label']
        except KeyError:
            self.label = self.type
        self.filepath = data.get('filepath', '')
        self.attrs = dict()
        for name, value in data.get('attrs', dict()).items():
            if isinstance(value, dict):
                attr_traits = value.get('traits', set())
                attr_val = value.get('value', None)
            else:
                attr_traits = set()
                attr_val = value
            self.attrs[name] = entityd.entityupdate.UpdateAttr(name,
                                                               attr_val,
                                                               attr_traits)
        self.children = [self._make_rel(c) for c in data.get('children', [])]
        self.parents = [self._make_rel(p) for p in data.get('parents', [])]

    @property
    def type(self):
        """Return the type of the entity."""
        return self._type

    @type.setter
    def type(self, value):
        """Validate and set the type of the entity."""
        if '/' in value:
            raise ValidationError("'/' not allowed in type field.")
        else:
            self._type = value

    @staticmethod
    def _make_rel(relation):
        """Validate a list of relations, converting them to RelDesc tuples.

        :param rel_list: A list of relations which will be validated

        Returns a list of RelDesc tuples matching the dictionaries in
        the original rel_list. A RelDesc tuple has two fields, `type`
        is required and specifies the type of entitiy to find; `attrs`
        is a dictionary whose keys are attribute names, and whose
        values are regular expressions used to match on the attribute
        values for entitiy matching.

        Raises ValidationError if any of the relations cannot be validated.

        """
        if isinstance(relation, RelDesc):
            return relation
        elif isinstance(relation, dict):
            try:
                relation = RelDesc(relation.pop('type'), relation)
            except KeyError:
                raise ValidationError(
                    "'type' is required for relation definition")
            else:
                return relation
        else:
            raise ValidationError(
                'Bad relation description, expected dictionary, got {}'
                .format(type(relation).__name__))
