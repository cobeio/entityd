import pytest

from entityd.mixins import HostUEID


@pytest.fixture
def host_ueid(pm, host_entity_plugin):  # pylint: disable=unused-argument
    """A HostUEID instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.
    """
    host_ueid = HostUEID()
    pm.register(host_ueid, 'entityd.mixins.HostUEID')
    return host_ueid


def test_host_ueid(session, host_ueid):
    host_ueid.entityd_sessionstart(session)

    assert host_ueid.host_ueid
