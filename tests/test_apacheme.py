import os
import subprocess
import time

import cobe
import pytest
import requests

import entityd.apacheme
import entityd.core
import entityd.fileme
import entityd.hostme
import entityd.processme
from entityd.apacheme import ApacheNotFound
from entityd.apacheme import VHost


CONF_FILE = "made_up_conf.conf"


HTTPD_ROOT = "/made/up/path"


FULL_PATH_TO_CONF = os.path.join(HTTPD_ROOT, CONF_FILE)


APACHECTL__V = """\
Server version: Apache/2.4.7 (Ubuntu)
Server built:   Jul 22 2014 14:36:38
Server's Module Magic Number: 20120211:27
Server loaded:  APR 1.5.1-dev, APR-UTIL 1.5.3
Compiled using: APR 1.5.1-dev, APR-UTIL 1.5.3
Architecture:   64-bit
Server MPM:     event
  threaded:     yes (fixed thread count)
    forked:     yes (variable process count)
Server compiled with....
 -D APR_HAS_SENDFILE
 -D APR_HAS_MMAP
 -D APR_HAVE_IPV6 (IPv4-mapped addresses enabled)
 -D APR_USE_SYSVSEM_SERIALIZE
 -D APR_USE_PTHREAD_SERIALIZE
 -D SINGLE_LISTEN_UNSERIALIZED_ACCEPT
 -D APR_HAS_OTHER_CHILD
 -D AP_HAVE_RELIABLE_PIPED_LOGS
 -D DYNAMIC_MODULE_LIMIT=256
 -D HTTPD_ROOT="{}"
 -D SUEXEC_BIN="/usr/lib/apache2/suexec"
 -D DEFAULT_PIDLOG="/var/run/apache2.pid"
 -D DEFAULT_SCOREBOARD="logs/apache_runtime_status"
 -D DEFAULT_ERRORLOG="logs/error_log"
 -D AP_TYPES_CONFIG_FILE="mime.types"
 -D SERVER_CONFIG_FILE="{}"
""".format(HTTPD_ROOT, CONF_FILE)


def has_running_apache():
    try:
        apache = entityd.apacheme.Apache()
        apache.performance_data()
    except ApacheNotFound:
        return False
    else:
        return True


running_apache = pytest.mark.skipif(not has_running_apache(),
                                    reason="Apache isn't reachable.")


def has_apachectl():
    try:
        binary = entityd.apacheme.Apache.apachectl_binary()
    except ApacheNotFound:
        return False
    return bool(binary)


apachectl = pytest.mark.skipif(not has_apachectl(),
                               reason="Local Apache binaries needed.")


@pytest.fixture
def fileme(pm, session, host_entity_plugin):  # pylint: disable=unused-argument
    gen = entityd.fileme.FileEntity()
    pm.register(gen, 'entityd.fileme.FileEntity')
    gen.entityd_configure(session.config)
    gen.entityd_sessionstart(session)
    return gen


@pytest.fixture
def entitygen(pm, session, host_entity_plugin):  # pylint: disable=unused-argument
    """A entityd.apacheme.ApacheEntity instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.

    """
    gen = entityd.apacheme.ApacheEntity()
    pm.register(gen, 'entityd.apacheme.ApacheEntity')
    gen.entityd_sessionstart(session)
    return gen


@pytest.fixture
def patched_entitygen(request, monkeypatch, pm, session, host_entity_plugin):  # pylint: disable=unused-argument
    """A entityd.apacheme.ApacheEntity instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.

    This is patched so that it doesn't rely on a live Apache server.

    """
    gen = entityd.apacheme.ApacheEntity()
    pm.register(gen, 'entityd.apacheme.ApacheEntity')
    gen.entityd_sessionstart(session)

    procgen = entityd.processme.ProcessEntity()
    pm.register(procgen, 'entityd.processme.ProcessEntity')
    procgen.entityd_sessionstart(session)
    request.addfinalizer(procgen.entityd_sessionfinish)
    mock_apache_process = entityd.EntityUpdate('Process')
    mock_apache_process.attrs.set('pid', 123, traits={'entity:id'})
    mock_apache_process.attrs.set('ppid', 1, traits={'entity:id'})
    mock_apache_process.attrs.set('starttime', 456, traits={'entity:id'})
    mock_apache_process.attrs.set('binary', 'apache2')

    mock_apache_child_process = entityd.EntityUpdate('Process')
    mock_apache_child_process.attrs.set('pid', 1234, traits={'entity:id'})
    mock_apache_child_process.attrs.set('ppid', 123, traits={'entity:id'})
    mock_apache_child_process.attrs.set('starttime', 456, traits={'entity:id'})
    mock_apache_child_process.attrs.set('binary', 'apache2')
    monkeypatch.setattr(procgen,
                        'filtered_processes',
                        pytest.Mock(return_value=[mock_apache_process,
                                                  mock_apache_child_process]))
    server_status_output = '\n'.join([
        'Total Accesses: 1081',
        'Total kBytes: 704',
        'CPULoad: .00384508',
        'Uptime: 1035348',
        'ReqPerSec: .00104409',
        'BytesPerSec: .696284',
        'BytesPerReq: 666.879',
        'BusyWorkers: 1',
        'IdleWorkers: 49',
        'ConnsTotal: 0',
        'ConnsAsyncWriting: 0',
        'ConnsAsyncKeepAlive: 0',
        'ConnsAsyncClosing: 0',
        'Scoreboard: _______________W__________________________________'
        '..............................................................'
        '......................................'
    ])

    response_obj = pytest.Mock(text=server_status_output)
    get_func = pytest.Mock(return_value=response_obj)
    monkeypatch.setattr(entityd.apacheme.Apache,
                        'get_apache_status',
                        get_func)

    monkeypatch.setattr(entityd.apacheme.Apache,
                        'apachectl_binary',
                        pytest.Mock(return_value='apachectl'))
    monkeypatch.setattr(entityd.apacheme.Apache,
                        'apache_binary',
                        pytest.Mock(return_value='apache2'))

    monkeypatch.setattr(entityd.apacheme.Apache,
                        'version',
                        'Apache/2.4.7 (Ubuntu)')
    monkeypatch.setattr(entityd.apacheme.Apache,
                        'apache_config',
                        pytest.Mock(return_value=FULL_PATH_TO_CONF))
    monkeypatch.setattr(entityd.apacheme.Apache,
                        'check_config',
                        pytest.Mock(return_value=True))
    monkeypatch.setattr(entityd.apacheme.Apache,
                        'config_last_modified',
                        pytest.Mock(return_value=time.time()))
    monkeypatch.setattr(
        entityd.apacheme.Apache,
        'vhosts',
        pytest.Mock(return_value={VHost('localhost', 80,
                                        '/etc/apache2/apache2.conf')}))
    return gen


@pytest.fixture
def apache():
    entityd.apacheme.Apache._apache_binary = 'apache2'
    entityd.apacheme.Apache._apachectl_binary = 'apachectl'
    return entityd.apacheme.Apache()


def test_configure(entitygen, config):
    entitygen.entityd_configure(config)
    assert config.entities['Apache'].obj is entitygen


@running_apache
@apachectl
def test_find_entity(request, entitygen):
    procgen = entityd.processme.ProcessEntity()
    entitygen.session.pluginmanager.register(procgen,
                                             'entityd.processme.ProcessEntity')
    procgen.entityd_sessionstart(entitygen.session)
    request.addfinalizer(procgen.entityd_sessionfinish)
    entities = entitygen.entityd_find_entity('Apache', None)
    count = 0
    for entity in entities:
        assert entity.metype == 'Apache'
        assert 'Apache/2' in entity.attrs.get('version').value
        assert os.path.isfile(entity.attrs.get('config_path').value)
        count += 1
    assert count


def test_find_entity_mocked_apache(patched_entitygen):
    entities = patched_entitygen.entityd_find_entity('Apache', None)
    count = 0
    for entity in entities:
        assert entity.metype == 'Apache'
        assert 'Apache/2' in entity.attrs.get('version').value
        for id_attr in ['host', 'config_path']:
            assert 'entity:id' in entity.attrs.get(id_attr).traits
        count += 1
    assert count


def test_find_entity_no_apache_running(patched_entitygen, monkeypatch):
    monkeypatch.setattr(entityd.apacheme.Apache, 'get_apache_status',
                        pytest.Mock(side_effect=ApacheNotFound))
    gen = patched_entitygen.entityd_find_entity('Apache', None)
    assert list(gen) == []


def test_find_entity_no_apachectl_binary(patched_entitygen, monkeypatch):
    procgen = patched_entitygen.session.pluginmanager.getplugin(
        'entityd.processme.ProcessEntity').obj
    monkeypatch.setattr(procgen,
                        'filtered_processes',
                        pytest.Mock(return_value=[]))
    monkeypatch.setattr(entityd.apacheme.Apache, 'apachectl_binary',
                        pytest.Mock(side_effect=ApacheNotFound))
    gen = patched_entitygen.entityd_find_entity('Apache', None)
    assert list(gen) == []


def test_find_entity_no_apache_binary(patched_entitygen, monkeypatch):
    procgen = patched_entitygen.session.pluginmanager.getplugin(
        'entityd.processme.ProcessEntity').obj
    monkeypatch.setattr(procgen,
                        'filtered_processes',
                        pytest.Mock(return_value=[]))
    monkeypatch.setattr(entityd.apacheme.Apache, 'apache_binary',
                        pytest.Mock(side_effect=ApacheNotFound))
    gen = patched_entitygen.entityd_find_entity('Apache', None)
    assert list(gen) == []


def test_find_entity_with_attrs():
    with pytest.raises(LookupError):
        entityd.apacheme.ApacheEntity().entityd_find_entity('Apache', {})


def test_entity_deleted_installed(patched_entitygen, monkeypatch):
    gen = patched_entitygen.entityd_find_entity('Apache', None)
    last_entity = next(gen)
    assert last_entity.metype == 'Apache'
    procgen = patched_entitygen.session.pluginmanager.getplugin(
        'entityd.processme.ProcessEntity').obj
    monkeypatch.setattr(procgen, 'filtered_processes',
                        pytest.Mock(return_value=[]))
    gen = patched_entitygen.entityd_find_entity('Apache', None)
    with pytest.raises(StopIteration):
        _ = next(gen)


def test_entity_deleted_running(patched_entitygen, monkeypatch):
    gen = patched_entitygen.entityd_find_entity('Apache', None)
    last_entity = next(gen)
    assert last_entity.metype == 'Apache'
    monkeypatch.setattr(entityd.apacheme.Apache, 'get_apache_status',
                        pytest.Mock(side_effect=ApacheNotFound))
    gen = patched_entitygen.entityd_find_entity('Apache', None)
    with pytest.raises(StopIteration):
        _ = next(gen)


def test_apache_entity_label(patched_entitygen):
    entities = patched_entitygen.entityd_find_entity('Apache', None)
    count = 0
    for entity in entities:
        assert entity.metype == 'Apache'
        if not entity.exists:
            continue
        assert entity.label.startswith('Apache')
        count += 1
    assert count


# pylint: disable=too-many-locals
def test_apache_not_found(patched_entitygen, monkeypatch):
    monkeypatch.setattr(entityd.apacheme.Apache, '__init__',
                        pytest.Mock(side_effect=ApacheNotFound))
    entities = patched_entitygen.entityd_find_entity('Apache', None)
    with pytest.raises(StopIteration):
        next(entities)


def test_relations(monkeypatch, tmpdir, pm, session, kvstore,  # pylint: disable=unused-argument
                   fileme, patched_entitygen):
    gen = patched_entitygen

    procent = patched_entitygen.session.pluginmanager.getplugin(
        'entityd.processme.ProcessEntity').obj

    hostgen = entityd.hostme.HostEntity()
    pm.register(hostgen, name='entityd.hostme')
    hostgen.entityd_sessionstart(session)
    hosts = hostgen.entityd_find_entity('Host', None)

    # The process entity is patched to return mocked processes
    procs = set(procent.entityd_find_entity(
        'Process', attrs={'binary': 'apache2'}))

    conf_file = tmpdir.join('apache2.conf')
    with conf_file.open('w') as f:
        f.write('test')
    monkeypatch.setattr(entityd.apacheme.Apache, 'apache_config',
                        pytest.Mock(return_value=str(conf_file)))

    conf_ent = next(
        fileme.entityd_find_entity('File', attrs={'path': str(conf_file)})
    )

    conf_file = tmpdir.join('apache2.conf')
    with conf_file.open('w') as f:
        f.write('test')

    monkeypatch.setattr(entityd.apacheme.Apache,
                        'apache_config',
                        pytest.Mock(return_value=str(conf_file)))

    conf_ent = next(
        fileme.entityd_find_entity('File', attrs={'path': str(conf_file)})
    )

    entity = next(gen.entityd_find_entity('Apache', attrs=None))
    assert len(entity.children._relations) == 3
    assert len([p for p in procs if p.ueid in entity.children._relations]) == 1
    assert conf_ent.ueid in entity.children._relations

    vhost = entityd.EntityUpdate('ApacheVHost')
    vhost.attrs.set('address', 'localhost', traits={'entity:id'})
    vhost.attrs.set('port', 80, traits={'entity:id'})
    vhost.attrs.set('apache', str(entity.ueid), traits={'entity:id'})
    assert vhost.ueid in entity.children._relations

    assert len(entity.parents._relations) == 1
    assert entity.parents._relations == set(host.ueid for host in hosts)


def test_config_file_returned_separately(pm, session, kvstore,  # pylint: disable=unused-argument
                                         patched_entitygen, fileme, tmpdir,
                                         monkeypatch):
    gen = patched_entitygen

    hostgen = entityd.hostme.HostEntity()
    pm.register(hostgen, name='entityd.hostme')
    hostgen.entityd_sessionstart(session)

    conf_file = tmpdir.join('apache2.conf')
    with conf_file.open('w') as f:
        f.write('test')

    monkeypatch.setattr(entityd.apacheme.Apache,
                        'apache_config',
                        pytest.Mock(return_value=str(conf_file)))

    conf_ent = next(
        fileme.entityd_find_entity('File', attrs={'path': str(conf_file)}))

    entities = list(gen.entityd_find_entity('Apache',
                                            attrs=None,
                                            include_ondemand=True))
    assert conf_ent.ueid in [e.ueid for e in entities if e.metype == 'File']
    assert [e for e in entities if e.metype == 'Apache']


def test_vhost_returned_separately(pm, session, kvstore,  # pylint: disable=unused-argument
                                   patched_entitygen):
    gen = patched_entitygen

    hostgen = entityd.hostme.HostEntity()
    pm.register(hostgen, name='entityd.hostme')
    hostgen.entityd_sessionstart(session)
    apache = next(gen.entityd_find_entity('Apache',
                                          attrs=None,
                                          include_ondemand=False))
    entities = list(gen.entityd_find_entity('Apache',
                                            attrs=None,
                                            include_ondemand=True))
    vhost = entityd.EntityUpdate('ApacheVHost')
    vhost.attrs.set('address', 'localhost', traits={'entity:id'})
    vhost.attrs.set('port', 80, traits={'entity:id'})
    vhost.attrs.set('apache', str(apache.ueid), traits={'entity:id'})

    ueids = [e.ueid for e in entities if e.metype == 'ApacheVHost']
    assert vhost.ueid in ueids
    assert [e for e in entities if e.metype == 'Apache']


def test_config_path_from_file(apache, monkeypatch):
    monkeypatch.setattr(subprocess, 'check_output',
                        pytest.Mock(return_value=APACHECTL__V))
    apache._apachectl_binary = 'apachectl'
    assert apache.config_path == FULL_PATH_TO_CONF


def test_config_path(apache, monkeypatch):
    apache._apachectl_binary = 'apachectl'
    monkeypatch.setattr(apache, 'apache_config',
                        pytest.Mock(return_value=FULL_PATH_TO_CONF))
    assert apache.config_path == FULL_PATH_TO_CONF


def test_config_path_fails(apache, monkeypatch):
    monkeypatch.setattr(subprocess, 'check_output', pytest.Mock(
        side_effect=subprocess.CalledProcessError(1, '')))
    apache._apachectl_binary = 'apachectl'
    apache.main_process = None
    with pytest.raises(ApacheNotFound):
        apache.apache_config()


def test_config_path_nobinary(apache, monkeypatch):
    monkeypatch.setattr(entityd.apacheme.Apache, 'apachectl_binary',
                        pytest.Mock(side_effect=ApacheNotFound))
    apache.main_process = None
    with pytest.raises(ApacheNotFound):
        apache.apache_config()


def test_config_path_not_set(monkeypatch):
    apache = entityd.apacheme.Apache()
    monkeypatch.setattr(subprocess, 'check_output', pytest.Mock(
        return_value='No useful output'))
    apache._apachectl_binary = 'httpd'
    with pytest.raises(ApacheNotFound):
        _ = apache.config_path


def test_config_path_from_proc(monkeypatch):
    monkeypatch.setattr(subprocess, 'check_output', pytest.Mock(
        return_value=APACHECTL__V))
    proc = entityd.EntityUpdate('Process')
    proc.attrs.set('command', 'apache2 -f /path/to.conf')
    apache = entityd.apacheme.Apache(proc)
    apache._apachectl_binary = 'apachectl'
    assert apache.config_path == '/path/to.conf'


def test_apache_config_no_output(monkeypatch):
    apache = entityd.apacheme.Apache()
    monkeypatch.setattr(subprocess,
                        'check_output', pytest.Mock(return_value=''))
    with pytest.raises(ApacheNotFound):
        apache.apache_config()


def test_root_dir_from_proc(monkeypatch):
    monkeypatch.setattr(subprocess, 'check_output', pytest.Mock(
        return_value=APACHECTL__V))
    proc = entityd.EntityUpdate('Process')
    proc.attrs.set('command', 'apache2 -d /path/to/rootdir')
    apache = entityd.apacheme.Apache(proc)
    apache._apachectl_binary = 'apachectl'
    assert apache.config_path.startswith('/path/to/rootdir/')


def test_conf_file_from_proc(monkeypatch):
    monkeypatch.setattr(subprocess, 'check_output', pytest.Mock(
        return_value=APACHECTL__V))
    proc = entityd.EntityUpdate('Process')
    proc.attrs.set('command', 'apache2 -f apacheconfig.conf')
    apache = entityd.apacheme.Apache(proc)
    apache._apachectl_binary = 'apachectl'
    assert apache.config_path.endswith('apacheconfig.conf')


def test_config_check(apache, monkeypatch):
    apache._apachectl_binary = 'apachectl'
    apache._config_path = FULL_PATH_TO_CONF
    monkeypatch.setattr(subprocess, 'check_call',
                        pytest.Mock(return_value=0))
    assert apache.check_config() is True


def test_config_check_fails(apache, monkeypatch):
    apache._apachectl_binary = 'apachectl'
    apache._config_path = FULL_PATH_TO_CONF
    monkeypatch.setattr(subprocess, 'check_call',
                        pytest.Mock(return_value=-1))
    assert apache.check_config() is False

    monkeypatch.setattr(
        subprocess, 'check_call',
        pytest.Mock(side_effect=subprocess.CalledProcessError(-1, '')))
    assert apache.check_config() is False


def test_apachectl_binary_found(monkeypatch):
    entityd.apacheme.Apache._apachectl_binary = None
    monkeypatch.setattr(subprocess, 'check_call',
                        pytest.Mock(return_value=0))
    assert entityd.apacheme.Apache.apachectl_binary() == 'apachectl'


def test_apachectl_binary_not_there(monkeypatch):
    entityd.apacheme.Apache._apachectl_binary = None
    monkeypatch.setattr(subprocess, 'check_call',
                        pytest.Mock(side_effect=FileNotFoundError))
    with pytest.raises(ApacheNotFound):
        _ = entityd.apacheme.Apache.apachectl_binary()


def test_apachectl_binary_fails(monkeypatch):
    entityd.apacheme.Apache._apachectl_binary = None
    monkeypatch.setattr(
        subprocess, 'check_call',
        pytest.Mock(side_effect=subprocess.CalledProcessError(-1, ''))
    )
    binary = entityd.apacheme.Apache.apachectl_binary()
    assert binary == 'apachectl'


def test_apache_binary_found(monkeypatch):
    entityd.apacheme.Apache._apache_binary = None
    monkeypatch.setattr(subprocess, 'check_call',
                        pytest.Mock(return_value=0))
    assert entityd.apacheme.Apache.apache_binary() == 'apache2'


def test_apache_binary_not_there(monkeypatch):
    entityd.apacheme.Apache._apache_binary = None
    monkeypatch.setattr(subprocess, 'check_call',
                        pytest.Mock(side_effect=FileNotFoundError))
    with pytest.raises(ApacheNotFound):
        _ = entityd.apacheme.Apache.apache_binary()


def test_apache_binary_fails(monkeypatch):
    entityd.apacheme.Apache._apache_binary = None
    monkeypatch.setattr(
        subprocess, 'check_call',
        pytest.Mock(side_effect=subprocess.CalledProcessError(-1, ''))
    )
    binary = entityd.apacheme.Apache.apache_binary()
    assert binary == 'apache2'


def test_apache_binary_fails_first(monkeypatch):
    entityd.apacheme.Apache._apache_binary = None
    def fail_once_then_succeed(*args, **kwargs):  # pylint: disable=unused-argument
        monkeypatch.setattr(subprocess, 'check_call',
                            pytest.Mock(return_value=0))
        raise FileNotFoundError()

    monkeypatch.setattr(
        subprocess, 'check_call',
        pytest.Mock(side_effect=fail_once_then_succeed)
    )
    binary = entityd.apacheme.Apache.apache_binary()
    assert binary == 'httpd'


def test_config_last_modified(apache, tmpdir):
    t = time.time()
    time.sleep(.1)
    tmpfile = tmpdir.join('apache.conf')
    apache._config_path = str(tmpfile)
    with tmpfile.open('w') as f:
        f.write("Test at: {}".format(t))

    assert t < apache.config_last_modified()
    assert apache.config_last_modified() < time.time()


def test_apaches_on_different_hosts_have_different_ueids(patched_entitygen):
    patched_entitygen._host_ueid = cobe.UEID('a' * 32)
    entity1 = next(patched_entitygen.entityd_find_entity('Apache', attrs=None))
    patched_entitygen._host_ueid = cobe.UEID('b' * 32)
    entity2 = next(patched_entitygen.entityd_find_entity('Apache', attrs=None))
    assert entity1.ueid != entity2.ueid


def test_version(apache, monkeypatch):
    monkeypatch.setattr(subprocess, 'check_output',
                        pytest.Mock(return_value=APACHECTL__V))
    apache._apachectl_binary = 'apachectl'
    assert apache.version == 'Apache/2.4.7 (Ubuntu)'
    assert apache.version is apache.version  # Test that it is cached


def test_performance_data(apache, monkeypatch):
    server_status_output = '\n'.join([
        'Total Accesses: 1081',
        'Total kBytes: 704',
        'CPULoad: .00384508',
        'Uptime: 1035348',
        'ReqPerSec: .00104409',
        'BytesPerSec: .696284',
        'BytesPerReq: 666.879',
        'BusyWorkers: 1',
        'IdleWorkers: 49',
        'ConnsTotal: 0',
        'ConnsAsyncWriting: 0',
        'ConnsAsyncKeepAlive: 0',
        'ConnsAsyncClosing: 0',
        'Scoreboard: _______________W__________________________________'
        '..............................................................'
        '......................................'
    ])

    response_obj = pytest.Mock(text=server_status_output)
    get_func = pytest.Mock(return_value=response_obj)
    monkeypatch.setattr(requests,
                        'get',
                        get_func)
    monkeypatch.setattr(apache,
                        'vhosts',
                        pytest.Mock(return_value=[VHost('localhost', 80, '')]))
    perfdata = apache.performance_data()

    get_func.assert_called_with('http://localhost:80/server-status?auto')

    assert perfdata['TotalAccesses'] == (1081, {'metric:counter'})
    assert perfdata['TotalkBytes'] == (704, {'metric:counter', 'unit:bytes'})
    assert perfdata['CPULoad'] == (0.00384508, {'metric:gauge'})
    assert perfdata['Uptime'] == (1035348, {'metric:counter',
                                            'time:duration', 'unit:seconds'})
    assert perfdata['ReqPerSec'] == (0.00104409, {'metric:gauge'})
    assert perfdata['BytesPerSec'] == (0.696284,
                                       {'metric:gauge', 'unit:bytes'})
    assert perfdata['BytesPerReq'] == (666.879, {'metric:gauge', 'unit:bytes'})
    assert perfdata['BusyWorkers'] == (1, {'metric:gauge'})
    assert perfdata['IdleWorkers'] == (49, {'metric:gauge'})
    assert perfdata['ConnsTotal'] == (0, {'metric:gauge'})
    assert perfdata['ConnsAsyncWriting'] == (0, {'metric:gauge'})
    assert perfdata['ConnsAsyncKeepAlive'] == (0, {'metric:gauge'})
    assert perfdata['ConnsAsyncClosing'] == (0, {'metric:gauge'})
    assert perfdata['workers:sending'] == (1, {'metric:gauge'})
    assert perfdata['workers:waiting'] == (49, {'metric:gauge'})
    assert perfdata['workers:open'] == (100, {'metric:gauge'})

    assert sum(perfdata[key][0] for key in [
        'workers:waiting',
        'workers:starting',
        'workers:reading',
        'workers:sending',
        'workers:keepalive',
        'workers:dns',
        'workers:closing',
        'workers:logging',
        'workers:finishing',
        'workers:idle'
    ]) == sum([perfdata['BusyWorkers'][0],
               perfdata['IdleWorkers'][0]])


def test_performance_data_fails(apache, monkeypatch):
    monkeypatch.setattr(requests, 'get',
                        pytest.Mock(
                            side_effect=requests.exceptions.ConnectionError))
    monkeypatch.setattr(
        apache, 'vhosts',
        pytest.Mock(return_value=set([VHost('incorrect.com', 1111, '')])))
    with pytest.raises(ApacheNotFound):
        apache.performance_data()


def test_listening_addresses(apache, monkeypatch):
    # pylint: disable=line-too-long
    apache._apachectl_binary = 'apachectl'
    apache._config_path = '/path/to.conf'
    monkeypatch.setattr(subprocess, 'check_output',
                        pytest.Mock(return_value="""\
        VirtualHost configuration:
            *:8881                 localhost (/etc/apache2/sites-enabled/001-default-copy.conf:2)
            127.0.0.1:8882                 127.0.0.1 (/etc/apache2/sites-enabled/foo.conf:2)
            127.0.0.1:8883                 127.0.0.1 ()
    """))
    vhost_8881 = VHost('localhost', 8881,
                       '/etc/apache2/sites-enabled/001-default-copy.conf')
    vhost_8882 = VHost('127.0.0.1', 8882, '/etc/apache2/sites-enabled/foo.conf')
    vhost_8883 = VHost('127.0.0.1', 8883, '')
    assert vhost_8881 in apache.vhosts()
    assert vhost_8882 in apache.vhosts()
    assert vhost_8883 in apache.vhosts()


def test_get_all_includes(tmpdir):
    config = r"""\
        ServerName localhost
        Mutex file:${APACHE_LOCK_DIR} default
        PidFile ${APACHE_PID_FILE}
        Timeout 300
        KeepAlive On
        MaxKeepAliveRequests 100
        KeepAliveTimeout 5
        User ${APACHE_RUN_USER}
        Group ${APACHE_RUN_GROUP}
        HostnameLookups Off
        ErrorLog ${APACHE_LOG_DIR}/error.log
        LogLevel warn
        IncludeOptional mods-enabled/*.load
        IncludeOptional mods-enabled/*.conf
        Include ports.conf

        <Directory />
            Options FollowSymLinks
            AllowOverride None
            Require all denied
        </Directory>

        <Directory /usr/share>
            AllowOverride None
            Require all granted
        </Directory>

        <Directory /var/www/>
            Options Indexes FollowSymLinks
            AllowOverride None
            Require all granted
        </Directory>


        AccessFileName .htaccess

        <FilesMatch "^\.ht">
            Require all denied
        </FilesMatch>


        LogFormat "%v:%p %h %l %u %t \"%r\" %>s %O \"%{Referer}i\" \"%{User-Agent}i\"" vhost_combined
        LogFormat "%h %l %u %t \"%r\" %>s %O \"%{Referer}i\" \"%{User-Agent}i\"" combined
        LogFormat "%h %l %u %t \"%r\" %>s %O" common
        LogFormat "%{Referer}i -> %U" referer
        LogFormat "%{User-agent}i" agent

        # Include of directories ignores editors' and dpkg's backup files,
        # see README.Debian for details.

        # Include generic snippets of statements
        IncludeOptional conf-enabled/*.conf

        # Include the virtual host configurations:
        IncludeOptional sites-enabled/*.conf
    """

    conf = tmpdir.join('apacheconf', 'apache2.conf')
    with conf.open('w', ensure=True) as f:
        f.write(config)

    site = tmpdir.join('apacheconf', 'sites-enabled', '000-default.conf')
    with site.open('w', ensure=True) as f:
        f.write("""\
        <VirtualHost *:80>
            ServerAdmin webmaster@localhost
            DocumentRoot /var/www/html
        </VirtualHost>
    """)
    apache = entityd.apacheme.Apache()
    apache._config_path = str(conf)
    includes = apache.find_all_includes()
    assert str(site) in includes
