
import pytest

import entityd.apacheme
import entityd.core


@pytest.fixture
def entitygen(pm):
    """A entityd.apacheme.apacheEntity instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.

    """
    gen = entityd.apacheme.ApacheEntity()
    pm.register(gen, 'entityd.apacheme.ApacheEntity')
    return gen


@pytest.fixture
def entity(entitygen):
    entity = next(entitygen.entityd_find_entity('Apache', None))
    return entity


def test_plugin_registered(pm):
    name = 'entityd.apacheme'
    entityd.apacheme.entityd_plugin_registered(pm, name)
    assert pm.isregistered('entityd.apacheme.ApacheEntity')


def test_configure(entitygen, config):
    entitygen.entityd_configure(config)
    assert config.entities['Apache'].obj is entitygen


def test_find_entity(entitygen):
    entities = entitygen.entityd_find_entity('Apache', None)
    count = 0
    for entity in entities:
        assert entity.metype == 'Apache'
        if entity.deleted:
            continue
        count += 1
    assert count


def test_config_check(entity):
    """Checks the Apache config.

    Currently relies on the system Apache install
    """
    assert entity.attrs.get('config_ok').value in [True, False]


def test_performance_data(entity):
    assert isinstance(entity.attrs.get('TotalAccesses').value, int)
    assert entity.attrs.get('TotalAccesses').value >= 0
    assert entity.attrs.get('Waiting').value >= 0



