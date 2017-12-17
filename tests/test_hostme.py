import functools
import platform
import socket
import time

import collections
import pytest

import act
import syskit

import entityd.hookspec
import entityd.hostme


@pytest.fixture(autouse=True)
def revert_mocking_of_cpuusage(mock_cpuusage):
    """Revert the mocking out of the CpuUsage calculation thread."""
    mock_cpuusage.revert()


@pytest.yield_fixture
def host_gen(monkeypatch):
    monkeypatch.setattr(entityd.hostme, "HostCpuUsage",
                        functools.partial(entityd.hostme.HostCpuUsage,
                                          interval=0.1))
    host_gen = entityd.hostme.HostEntity()
    session = pytest.Mock()
    host_gen.entityd_sessionstart(session)
    # Disable actual sqlite database persistence
    host_gen.session.svc.kvstore.get.side_effect = KeyError
    yield host_gen
    host_gen.entityd_sessionfinish()


@pytest.fixture(params=[True, False])
def host(host_gen, request, monkeypatch):
    """Create host entities.

    This is parametrised to create one host entity that would be generated by
    an entityd running in a container, and another from an entityd that is not
    in a container.
    """
    entities = list(host_gen.entityd_find_entity(name='Host', attrs=None))
    if request.param:
        monkeypatch.setattr(
            entityd.hostme.os.path, 'isfile', pytest.Mock(return_value=True))
    return entities[0]


def test_configure():
    config = pytest.Mock()
    entityd.hostme.HostEntity().entityd_configure(config)
    assert config.addentity.called_once_with('Host',
                                             'entityd.hostme.HostEntity')


def test_session_stored_on_start():
    session = pytest.Mock()
    he = entityd.hostme.HostEntity()
    he.entityd_sessionstart(session)
    assert he.session is session
    he.entityd_sessionfinish()


def test_find_entity_with_attrs():
    with pytest.raises(LookupError):
        entityd.hostme.HostEntity().entityd_find_entity('Host', {})


def test_metype(host):
    assert host.metype == 'Host'


def test_bootid(host):
    with open('/proc/sys/kernel/random/boot_id', 'r') as fp:
        assert host.attrs.get('bootid').value == fp.read().strip()
        assert host.attrs.get('bootid').traits == {'entity:id'}


def test_bootid_determined_only_once(host_gen):
    host_gen._bootid = 'testbootid'
    assert host_gen.bootid == 'testbootid'


def test_hostname_when_entityd_not_in_container(host_gen, monkeypatch):
    monkeypatch.setattr(entityd.hostme.os.path,
                        'isfile', pytest.Mock(return_value=False))
    entity = next(host_gen.entityd_find_entity(name='Host', attrs=None))
    assert entity.attrs.get('hostname').value == socket.gethostname()
    assert entity.attrs.get('hostname').traits == {'index'}


def test_hostname_when_entityd_in_container(host_gen, monkeypatch):
    monkeypatch.setattr(entityd.hostme.os.path,
                        'isfile', pytest.Mock(return_value=True))
    entity = next(host_gen.entityd_find_entity(name='Host', attrs=None))
    assert 'hostname' not in entity.attrs


def test_fqdn_when_entityd_not_in_container(host_gen, monkeypatch):
    monkeypatch.setattr(entityd.hostme.os.path,
                        'isfile', pytest.Mock(return_value=False))
    entity = next(host_gen.entityd_find_entity(name='Host', attrs=None))
    assert entity.attrs.get('fqdn').value == socket.getfqdn()
    assert entity.attrs.get('fqdn').traits == {'index'}


def test_fqdn_when_entityd_in_container(host_gen, monkeypatch):
    monkeypatch.setattr(entityd.hostme.os.path,
                        'isfile', pytest.Mock(return_value=True))
    entity = next(host_gen.entityd_find_entity(name='Host', attrs=None))
    assert 'fqdn' not in entity.attrs


def test_uptime(host):
    assert abs(host.attrs.get('uptime').value - int(syskit.uptime())) <= 1
    assert host.attrs.get('uptime').traits == {
        'time:duration', 'unit:seconds', 'metric:counter'}


def test_boottime(host):
    assert host.attrs.get('boottime').value == syskit.boottime().timestamp()
    assert host.attrs.get('boottime').traits == {'time:posix', 'unit:seconds'}


def test_os(host):
    assert host.attrs.get('os').value == platform.system()
    assert host.attrs.get('os').traits == {'index'}


def test_osversion(host):
    assert host.attrs.get('osversion').value == platform.release()
    assert host.attrs.get('osversion').traits == {'index'}


def test_free(host):
    memorystats = syskit.MemoryStats()
    free = memorystats.free + memorystats.buffers + memorystats.cached
    free *= 1024
    assert abs(host.attrs.get('free').value - free) < 1024 ** 2
    assert host.attrs.get('free').traits == {'unit:bytes', 'metric:gauge'}


def test_used(host):
    memorystats = syskit.MemoryStats()
    free = memorystats.free + memorystats.buffers + memorystats.cached
    used = memorystats.total - free
    used *= 1024
    assert abs(host.attrs.get('used').value - used) < 1024 ** 2
    assert host.attrs.get('used').traits == {'unit:bytes', 'metric:gauge'}


def test_total(host):
    memorystats = syskit.MemoryStats()
    total = memorystats.total
    total *= 1024
    assert host.attrs.get('total').value == total
    assert host.attrs.get('total').traits == {'metric:gauge', 'unit:bytes'}


def test_cpu_usage(host_gen):
    entities = list(host_gen.entityd_find_entity(name='Host', attrs=None))
    host = entities[0]
    assert isinstance(host.attrs.get('usr').value, float)
    for key in ['cpu:usr', 'cpu:sys', 'cpu:nice',
                'cpu:idle', 'cpu:iowait', 'cpu:irq',
                'cpu:softirq', 'cpu:steal']:
        with pytest.raises(KeyError):
            host.attrs.get(key)
    time.sleep(.1)
    entities = list(host_gen.entityd_find_entity(name='Host', attrs=None))
    host = entities[0]
    assert 99 < sum([host.attrs.get(key).value
                     for key in ['cpu:usr', 'cpu:sys', 'cpu:nice',
                                 'cpu:idle', 'cpu:iowait', 'cpu:irq',
                                 'cpu:softirq', 'cpu:steal']]) <= 100.1


def test_loadavg(host_gen):
    entities = list(host_gen.hosts())
    host = entities[0]
    loadavgs = (host.attrs.get('loadavg_1'),
                host.attrs.get('loadavg_5'),
                host.attrs.get('loadavg_15'))
    for av in loadavgs:
        assert isinstance(av.value, float)
        assert 'metric:gauge' in av.traits
        assert 0 < av.value < 16


def test_entity_label_when_not_in_container(host_gen, monkeypatch):
    monkeypatch.setattr(entityd.hostme.os.path, 'isfile', pytest.Mock(
        return_value=False))
    entity = next(host_gen.entityd_find_entity(name='Host', attrs=None))
    assert entity.label == socket.gethostname()


def test_entity_label_when_in_container(host_gen, monkeypatch):
    monkeypatch.setattr(entityd.hostme.os.path, 'isfile', pytest.Mock(
        return_value=True))
    entity = next(host_gen.entityd_find_entity(name='Host', attrs=None))
    assert entity.label == socket.gethostname()


CpuTimes = collections.namedtuple('CpuTimes',
                                  ['usr', 'nice', 'sys', 'idle', 'iowait',
                                   'irq', 'softirq', 'steal', 'guest',
                                   'guest_nice'])


class TestHostCpuUsage:

    @pytest.fixture
    def context(self):
        return act.zkit.new_context()

    @pytest.fixture
    def cpuusage(self, context):
        return entityd.hostme.HostCpuUsage(context, interval=0.1)

    def test_timer(self, monkeypatch, cpuusage):
        """Test the timer is firing, and triggers an update."""
        monkeypatch.setattr(cpuusage, '_update_times',
                            pytest.Mock(side_effect=cpuusage.stop))
        cpuusage.start()
        cpuusage.join()
        assert cpuusage._update_times.called

    def test_exception_logged(self, monkeypatch, cpuusage):
        monkeypatch.setattr(cpuusage, '_run',
                            pytest.Mock(side_effect=ZeroDivisionError))
        monkeypatch.setattr(cpuusage, '_log', pytest.Mock())
        stop = lambda self: monkeypatch.setattr(cpuusage,
                                                '_run', pytest.Mock())
        cpuusage._log.exception.side_effect = stop
        cpuusage.start()
        cpuusage.join(timeout=2)
        assert cpuusage._log.exception.called

    def test_first_update(self, cpuusage):
        # On the first update, percentages not included
        assert not cpuusage.last_cpu_times
        assert not cpuusage.last_attributes
        cpuusage._update_times()
        assert cpuusage.last_cpu_times
        for name, _, traits in cpuusage.last_attributes:
            assert not name.startswith('cpu:')
            assert 'unit:percent' not in traits

    def test_time_unchanged(self, monkeypatch, cpuusage):
        # On zero time change, percentages not included
        times = syskit.cputimes()
        monkeypatch.setattr(syskit, 'cputimes',
                            pytest.Mock(return_value=times))
        cpuusage._update_times()
        cpuusage._update_times()
        for name, _, traits in cpuusage.last_attributes:
            assert not name.startswith('cpu:')
            assert 'unit:percent' not in traits

    def test_time_calc_equal(self, monkeypatch, cpuusage):
        # If each time is increased by the same amount,
        # percentages are equal
        times = syskit.cputimes()
        monkeypatch.setattr(syskit, 'cputimes',
                            pytest.Mock(return_value=times))
        cpuusage._update_times()
        new_times = CpuTimes(*(val + 1 for val in times))
        monkeypatch.setattr(syskit, 'cputimes',
                            pytest.Mock(return_value=new_times))
        cpuusage._update_times()
        count = 0
        percentage = 100.0 / len(new_times)
        for name, value, traits in cpuusage.last_attributes:
            if name.startswith('cpu:'):
                assert 'unit:percent' in traits
                assert value == percentage
                count += 1
        assert count == len(new_times)

    def test_time_calc_single(self, monkeypatch, cpuusage):
        # If one time is increased, it should be 100%
        times = syskit.cputimes()
        monkeypatch.setattr(syskit, 'cputimes',
                            pytest.Mock(return_value=times))
        cpuusage._update_times()
        new_times = CpuTimes(*(val + int(i == 0)
                               for i, val in enumerate(times)))
        monkeypatch.setattr(syskit, 'cputimes',
                            pytest.Mock(return_value=new_times))
        cpuusage._update_times()
        count = 0
        for name, value, _ in cpuusage.last_attributes:
            if name == 'cpu:usr':
                assert value == 100
                count += 1
            elif name.startswith('cpu:'):
                assert value == 0
                count += 1
        assert count == len(new_times)
