"""Plugin for writing Entityd's state as a DOT file."""

import hashlib
import itertools
import pathlib

import entityd.pm
import logbook


log = logbook.Logger(__name__)


_COLOUR_PALETTE = [
    '#FFC86C',
    '#EA8279',
    '#8850A4',
    '#8EBFEF',
    '#F29C9C',
    '#AC98B6',
    '#AEAEAE',
    '#C2F488',
]


def _colour(string, *, exclude=None):
    """Select a colour from the palette.

    :param string: A string to use to select a colour.
    :type string: str:
    :param exclude: Colours to exclude from the palette.
    :type exclude: iterable of str
    """
    hash_ = hashlib.sha1(string.encode())
    hash_value = int(hash_.hexdigest(), 16)
    palette = [colour for colour
               in _COLOUR_PALETTE if colour not in (exclude or ())]
    return _COLOUR_PALETTE[hash_value % len(_COLOUR_PALETTE)]


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


@entityd.pm.hookimpl
def entityd_collection_after(session, updates):
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
    _write_dot(session.config.args.dot, set(entities.values()), relationships)


def _write_dot(path, entities, relationships):
    log.info('Writing DOT to {}', path)
    with path.open('w') as dot:
        segments = [
            _write_dot_header(),
            _write_dot_entities(entities),
            _write_dot_relationships(relationships),
            _write_dot_footer(),
        ]
        for chunk in itertools.chain(*segments):
            dot.write(chunk)
            dot.write('\n')
    log.info('Finished writing DOT to {}', path)


def _write_dot_header():
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
    for entity in entities:
        namespace = entity.metype.rsplit(':', 1)[0]
        type_ = entity.metype[len(namespace) + 1:]
        colour_border = _colour(namespace)
        if type_:
            colour_background = _colour(type_, exclude={colour_border})
        else:
            colour_background = "#ffffff"
        yield '"{0.ueid}" [label="{0.metype}\\n{0.label}", color="{1}" fillcolor="{2}"];'.format(entity, colour_border, colour_background)


def _write_dot_relationships(relationships):
    for (parent, child) in relationships:
        yield '"{0}" -> "{1}";'.format(parent, child)


def _write_dot_footer():
    yield '}'
