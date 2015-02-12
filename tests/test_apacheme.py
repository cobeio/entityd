import os
import subprocess
import time

import pytest
import requests

import entityd.apacheme
import entityd.core
import entityd.processme


def print_entity(entity):
    print("{} entity: {}".format(entity.metype, entity.ueid))
    print("  Time:", entity.timestamp)
    for attr in entity.attrs:
        print("  Attr:", attr)
    for rel in entity.children._relations | entity.parents._relations:
        print("  Rel:", rel)


def has_running_apache():
    try:
        entityd.apacheme._get_apache_status()
    except RuntimeError:
        return False
    else:
        return True


running_apache = pytest.mark.skipif(not has_running_apache(),
                                    reason="Apache isn't reachable.")


def has_apachectl():
    try:
        entityd.apacheme._apachectl_binary()
    except RuntimeError:
        return False
    else:
        return True


apachectl = pytest.mark.skipif(not has_apachectl(),
                               reason="Local Apache binaries needed.")


@pytest.fixture
def entitygen(pm, session):
    """A entityd.apacheme.ApacheEntity instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.

    """
    gen = entityd.apacheme.ApacheEntity()
    pm.register(gen, 'entityd.apacheme.ApacheEntity')
    gen.entityd_sessionstart(session)
    return gen


@pytest.fixture
def patched_entitygen(monkeypatch, pm, session):
    """A entityd.apacheme.ApacheEntity instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.

    This is patched so that it doesn't rely on a live Apache server.

    """
    gen = entityd.apacheme.ApacheEntity()
    pm.register(gen, 'entityd.apacheme.ApacheEntity')
    gen.entityd_sessionstart(session)

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
    monkeypatch.setattr(entityd.apacheme,
                        '_get_apache_status',
                        get_func)

    gen.apache._apachectl_binary = 'apachectl'

    gen.apache.version = pytest.Mock(
        return_value='Apache/2.4.7 (Ubuntu)')
    gen.apache._config_path = b'/etc/apache2/apache2.conf'
    gen.apache.check_config = pytest.Mock(
        return_value=True
    )
    return gen


@pytest.fixture
def apache():
    return entityd.apacheme.Apache()


def test_plugin_registered(pm):
    name = 'entityd.apacheme'
    entityd.apacheme.entityd_plugin_registered(pm, name)
    assert pm.isregistered('entityd.apacheme.ApacheEntity')


def test_configure(entitygen, config):
    entitygen.entityd_configure(config)
    assert config.entities['Apache'].obj is entitygen


@running_apache
@apachectl
def test_find_entity(entitygen):
    entities = entitygen.entityd_find_entity('Apache', None)
    count = 0
    for entity in entities:
        assert entity.metype == 'Apache'
        if entity.deleted:
            continue
        assert entity.attrs.get('version').value
        assert os.path.isfile(entity.attrs.get('config_path').value)
        count += 1
    assert count


def test_find_entity_mocked_apache(patched_entitygen):
    entities = patched_entitygen.entityd_find_entity('Apache', None)
    count = 0
    for entity in entities:
        assert entity.metype == 'Apache'
        if entity.deleted:
            continue
        assert entity.attrs.get('version').value
        assert os.path.isfile(entity.attrs.get('config_path').value)
        count += 1
    assert count


def test_find_entity_no_apache(patched_entitygen, monkeypatch):
    monkeypatch.setattr(entityd.apacheme, '_get_apache_status',
                        pytest.Mock(side_effect=RuntimeError))
    gen = patched_entitygen.entityd_find_entity('Apache', None)
    assert list(gen) == []


def test_find_entity_with_attrs():
    with pytest.raises(LookupError):
        entityd.apacheme.ApacheEntity().entityd_find_entity('Apache', {})


@running_apache
@apachectl
def test_relations(pm, session, kvstore, entitygen, apache):  # pylint: disable=unused-argument
    procent = entityd.processme.ProcessEntity()
    pm.register(procent,
                name='entityd.processme')
    procent.entityd_sessionstart(session)

    binary = apache.apache_binary
    processes = procent.entityd_find_entity('Process',
                                            attrs={'binary': binary})
    procs = set(processes)
    for p in procs:
        assert p.metype == 'Process'
        assert p.attrs.get('binary').value == binary
    entity = next(entitygen.entityd_find_entity('Apache', attrs=None))
    assert len(entity.children._relations) == len(procs)
    assert entity.children._relations == set(p.ueid for p in procs)


@apachectl
def test_config_path(apache):
    path = apache.config_path
    assert os.path.isfile(path)


def test_config_path_fails(monkeypatch):
    monkeypatch.setattr(subprocess, 'check_output', pytest.Mock(
        return_value=b'no config file here'))
    with pytest.raises(RuntimeError):
        entityd.apacheme._apache_config('httpd')


@apachectl
def test_config_check(apache):
    """Checks the Apache config.

    Currently relies on the system Apache install
    """
    assert apache.check_config() in [True, False]


@apachectl
def test_config_check_fails(apache, tmpdir):
    # The check goes via the apachectl binary - how to patch?
    path = tmpdir.join('apache.conf')

    with open(str(path), 'w') as f:
        f.write('-ServerName localhost\n')

    assert apache.check_config(str(path)) is False


@pytest.mark.parametrize("apachectl_binary,apache_binary", [
    ('apachectl', 'apache2'),
    ('apache2ctl', 'apache2'),
    ('httpd', 'httpd')
])
def test_apache_binary(apachectl_binary, apache_binary):
    assert entityd.apacheme._apache_binary(apachectl_binary) == apache_binary


def test_apachectl_binary_not_there(monkeypatch):
    monkeypatch.setattr(subprocess, 'check_output',
                        pytest.Mock(side_effect=FileNotFoundError))
    with pytest.raises(RuntimeError):
        entityd.apacheme._apachectl_binary()


def test_apachectl_binary_fails(monkeypatch):
    monkeypatch.setattr(
        subprocess, 'check_output',
        pytest.Mock(side_effect=subprocess.CalledProcessError(-1, ''))
    )
    with pytest.raises(RuntimeError):
        entityd.apacheme._apachectl_binary()


def test_config_last_modified(apache, tmpdir):
    t = time.time()
    time.sleep(.1)
    tmpfile = tmpdir.join('apache.conf')
    apache._config_path = str(tmpfile)
    with open(str(tmpfile), 'w') as f:
        f.write("Test at: {}".format(t))

    assert t < apache.config_last_modified()
    assert apache.config_last_modified() < time.time()


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

    perfdata = apache.performance_data()

    get_func.assert_called_with('http://127.0.1.1:80/server-status?auto')

    assert perfdata['TotalAccesses'] == 1081
    assert perfdata['TotalkBytes'] == 704
    assert perfdata['CPULoad'] == 0.00384508
    assert perfdata['Uptime'] == 1035348
    assert perfdata['ReqPerSec'] == 0.00104409
    assert perfdata['BytesPerSec'] == 0.696284
    assert perfdata['BytesPerReq'] == 666.879
    assert perfdata['BusyWorkers'] == 1
    assert perfdata['IdleWorkers'] == 49
    assert perfdata['ConnsTotal'] == 0
    assert perfdata['ConnsAsyncWriting'] == 0
    assert perfdata['ConnsAsyncKeepAlive'] == 0
    assert perfdata['ConnsAsyncClosing'] == 0

    assert perfdata['workers:sending'] == 1
    assert perfdata['workers:waiting'] == 49
    assert perfdata['workers:open'] == 100

    assert sum([
        perfdata['workers:waiting'],
        perfdata['workers:starting'],
        perfdata['workers:reading'],
        perfdata['workers:sending'],
        perfdata['workers:keepalive'],
        perfdata['workers:dns'],
        perfdata['workers:closing'],
        perfdata['workers:logging'],
        perfdata['workers:finishing'],
        perfdata['workers:idle']
    ]) == sum([perfdata['BusyWorkers'],
               perfdata['IdleWorkers']])


def test_performance_data_fails(apache, monkeypatch):
    monkeypatch.setattr(requests, 'get',
                        pytest.Mock(
                            side_effect=requests.exceptions.ConnectionError))
    with pytest.raises(RuntimeError):
        apache.performance_data()


@apachectl
def test_get_all_includes(apache):
    conf = apache.config_path
    includes = entityd.apacheme._find_all_includes(conf)
    assert b'/etc/apache2/sites-enabled/000-default.conf' in includes
