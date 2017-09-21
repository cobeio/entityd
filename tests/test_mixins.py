import pytest

from entityd.mixins import HostEntity


@pytest.fixture
def host_entity(pm, host_entity_plugin):  # pylint: disable=unused-argument
    """A HostUEID instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.
    """
    entity = HostEntity()
    pm.register(entity, 'entityd.mixins.HostEntity')
    return entity


def test_host_ueid(session, host_entity):
    host_entity.entityd_sessionstart(session)

    assert host_entity.host_ueid


def test_hostname(session, host_entity):
    host_entity.entityd_sessionstart(session)

    assert host_entity.hostname
