"""Plugin for writing Entityd's state as a DOT file."""

import enum
import hashlib
import itertools
import pathlib

import entityd.entityupdate
import entityd.pm

import logbook


log = logbook.Logger(__name__)


class _Palette:
    """A palette of colours.

    All colours must be expressed as hex-triplets. Their case will be
    normalised so only lowercase hexadecimal digits are used.

    :params colours: Ordered iterable of colours for the palette.
    :type colours: iterable of str

    :raises ValueError: If the given colours iterable is empty.
    """

    def __init__(self, colours):
        self._colours = tuple(colour.lower() for colour in colours)
        if not self._colours:
            raise ValueError('Colour palette can not be empty')

    def __getitem__(self, key):
        """Select a colour from the palette.

        :param key: A string to use to select a colour.
        :type key: str:

        :returns: A colour selected from the palette.
        """
        hash_ = hashlib.sha1(key.encode())
        hash_value = int(hash_.hexdigest(), 16)
        return self._colours[hash_value % len(self._colours)]

    @classmethod
    def default(cls):
        """The default palette."""
        return cls([
            '#FFC86C',
            '#EA8279',
            '#8850A4',
            '#8EBFEF',
            '#F29C9C',
            '#AC98B6',
            '#AEAEAE',
            '#C2F488',
        ])

    def exclude(self, *colours):
        """Exclude colours from the palette.

        This creates a new palette with the exlcuded colours removed.

        :param colours: Colours to exclude from the palette.
        :type colours: iterable of str

        :raises ValueError: If excluding the given colour would result
            in the new palette being empty.

        :returns: A copy of the current palette with the specified
            colours excluded.
        """
        colours_normalised = [colour.lower() for colour in colours]
        return self.__class__([
            colour for colour
            in self._colours if colour not in colours_normalised])


class _ForeignEntity(enum.Enum):
    """Behaviour for foreign entities in the graph."""

    DEFAULT = 'default'
    UEID = 'ueid'
    UEID_SHORT = 'short-ueid'
    EXCLUDE = 'exclude'

    def __str__(self):
        return self.value


@entityd.pm.hookimpl
def entityd_addoption(parser):
    parser.add_argument(
        '--dot',
        default=None,
        type=pathlib.Path,
        help=(
            'Path to write a DOT file of the collected entities to. '
            'The file will be continually overwritten with the most '
            'state whilst the agent is running. If no file is specified '
            'no DOT file will be written.'
        ),
    )
    parser.add_argument(
        '--dot-foreign',
        default=_ForeignEntity.DEFAULT,
        choices=sorted(
            (option for option in _ForeignEntity),
            key=lambda option: option.value,
        ),
        type=_ForeignEntity,
        help=(
            'How to treat references to entities which are not collected '
            'by this agent. The default behaviour will simply replace '
            'each foreign entity with a question mark.'
        ),
    )
    parser.add_argument(
        '--dot-pretty',
        action='store_true',
        help='Use pretty formatting when writing DOT file.',
    )


@entityd.pm.hookimpl
def entityd_collection_after(session, updates):
    """Optionally write a DOT file.

    If a DOT file path is specified by ``--dot``, it will be overwritten
    to represent the state of the updates from the collection cycle.

    If not DOT file path is specified, this function does nothing.
    """
    if session.config.args.dot is None:
        return
    entities = {}  # UEID : EntityUpdate
    relationships = set()
    for update in updates:
        entities[update.ueid] = update
    for entity in entities.values():
        for parent in entity.parents:
            relationships.add((parent, entity.ueid))
        for child in entity.children:
            relationships.add((entity.ueid, child))
    _process_foreign_references(
        session.config.args.dot_foreign, entities, relationships)
    _write_dot(
        session.config.args.dot,
        set(entities.values()),
        relationships,
        pretty=session.config.args.dot_pretty,
    )


def _process_foreign_references(method, entities, relationships):
    """Update and/or filter foreign references.

    The exact behaviour depends on the given processing method:

    - :attr:`_ForeignEntity.DEFAULT`: An entity is added for the
      foreign relation with a label set to a QUESTION MARK.
    - :attr:`_ForeignEntity.EXCLUDE`: The relationships are outright
      removed from the set of relationships.
    - :attr:`_ForeignEntity.UEID`: An entity is added for the
      foreign relation with a label set to the full UEID of the
      relation.
    - :attr:`_ForeignEntity.UEID_SHORT`: An entity is added for
      the foreign relation with a label set to the truncated UEID
      of the relation.

    .. warning::

        This will modify the given entity and relationship collections
        in place.

    :param entities: All local entity updates.
    :type entities: dict of cobe.UEID to entityd.entityupdate.EntityUpdate
    :param relationships: All local entity relationships.
    :type entities: set of tuple of cobe.UEID
    """
    if method is _ForeignEntity.EXCLUDE:
        foreigners = set()
        for relationship, _ in _foreign_references(entities, relationships):
            foreigners.add(relationship)
        relationships -= foreigners
    elif method is _ForeignEntity.DEFAULT:
        for _, relation in _foreign_references(entities, relationships):
            entity = entityd.entityupdate.EntityUpdate('', relation)
            entity.label = '?'
            entities[entity.ueid] = entity
    elif method is _ForeignEntity.UEID:
        for _, relation in _foreign_references(entities, relationships):
            entity = entityd.entityupdate.EntityUpdate('', relation)
            entity.label = str(relation)
            entities[entity.ueid] = entity
    elif method is _ForeignEntity.UEID_SHORT:
        for _, relation in _foreign_references(entities, relationships):
            entity = entityd.entityupdate.EntityUpdate('', relation)
            entity.label = str(relation)[:6]
            entities[entity.ueid] = entity


def _foreign_references(entities, relationships):
    """Find foreign entity references in a model.

    A foreign entity reference is defined as being a relationship,
    for which at least one relation doesn't exist in the given mapping
    of local entities.

    :param entities: All local entity updates.
    :type entities: dict of cobe.UEID to entityd.entityupdate.EntityUpdate
    :param relationships: All local entity relationships.
    :type entities: set of tuple of cobe.UEID

    :returns: An iterator of foreign entity references. Each foreign
        reference is represented by a tuple containing the relationship,
        and the offending UEID in the relationship.
    """
    for relationship in relationships:
        for relation in relationship:
            if relation not in entities:
                yield relationship, relation


def _pretty_print(segment):
    """Pretty print DOT segment chunks.

    Each chunk in the segment will be terminated by a line feed. If
    the the chunk ends with a LEFT CURLY BRACKET then subsequent chunks
    will be indented until a corresponding RIGHT CURLY BRACKET is found.

    :param segment: DOT chunk iterator.
    :type segment: iterator of str

    :returns: An iterator of strings that wraps around the given segment.
    """
    level = ''
    for chunk in segment:
        chunk_level = level
        if chunk.endswith('{'):
            level += '  '
        if chunk.startswith('}'):
            level = level[:-2]
            chunk_level = level
        yield chunk_level + chunk + '\n'


def _write_dot(path, entities, relationships, *, pretty=False):
    """Write model state to a DOT file."""
    log.info('Writing DOT to {}', path)
    with path.open('w') as dot:
        segments = itertools.chain(
            _write_dot_header(),
            _write_dot_entities(entities),
            _write_dot_relationships(relationships),
            _write_dot_footer(),
        )
        for chunk in _pretty_print(segments) if pretty else segments:
            dot.write(chunk)
    log.info('Finished writing DOT to {}', path)


def _write_dot_header():
    """Write a DOT digraph header.

    Numerous attributes are also set to control the rendering as the
    defaults are somewhat ugly.
    """
    yield 'digraph G {'
    yield 'graph [overlap=prism];'
    yield 'graph [rankdir=LR];'
    yield 'graph [splines=true];'
    yield 'graph [bgcolor="#ffffff"];'
    yield 'node [color=white];'
    yield 'node [fillcolor=white];'
    yield 'node [shape=box];'
    yield 'node [style=filled];'
    yield 'edge [arrowhead=open];'


def _write_dot_entities(entities):
    """Write DOT nodes.

    A node is created for each entity. The nodes will always be written
    in a consistent order. However, the exact sort order is opaque.

    :param entities: Entities to write as DOT nodes.
    :type entities: set of entityd.entityupdate.EntityUpdate
    """
    ordered = sorted(entities, key=lambda entity: str(entity.ueid))
    for entity in ordered:
        namespace = entity.metype.rsplit(':', 1)[0]
        type_ = entity.metype[len(namespace) + 1:]
        palette = _Palette.default()
        attributes = {
            'label': entity.label or '',
            'color': palette[namespace],
            'fillcolor': '#ffffff',
        }
        if type_:
            attributes['fillcolor'] = \
                palette.exclude(attributes['color'])[type_]
        if entity.metype:
            attributes['label'] = entity.metype + '\\n' + attributes['label']
            attributes['label'] = attributes['label'].rstrip('\\n')
        attributes_formatted = []
        for attribute in sorted(attributes):
            attributes_formatted.append(
                '{0}="{1}"'.format(attribute, attributes[attribute]))
        node_id = '"{entity.ueid}"'.format(entity=entity)
        node_attributes = ', '.join(attributes_formatted)
        yield node_id + ' [' + node_attributes + '];'


def _write_dot_relationships(relationships):
    """Write DOT relationships.

    The relationships will always be written in a consistent order.
    However, the exact sort order is opaque.

    :param relationships: Relationships to write as DOT.
    :type relationships: set of tuples of cobe.UEID
    """
    ordered = sorted(
        relationships, key=lambda relationship: str(relationship[0]))
    for (parent, child) in ordered:
        yield '"{0}" -> "{1}";'.format(parent, child)


def _write_dot_footer():
    """Write a DOT digraph footer."""
    yield '}'
