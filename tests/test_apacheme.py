import os
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


def has_apache():
    # TODO: should distinguish between a running, and an installed apache
    apache = entityd.apacheme.Apache()
    try:
        apache.apache_binary
    except RuntimeError:
        return False
    else:
        return True


apache = pytest.mark.skipif(not has_apache(), reason="Local Apache needed.")


def has_apachectl():
    try:
        entityd.apacheme.apachectl_binary()
    except RuntimeError:
        return False
    else:
        return True


apachectl = pytest.mark.skipif(not has_apache(), reason="Local Apache needed.")


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
    return gen


@pytest.fixture
def entity(patched_entitygen):
    entity = next(patched_entitygen.entityd_find_entity('Apache', None))
    return entity


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



# TODO: Separate tests that use actual Apache, and those that don't. Try to
# TODO: remove that dependency.
# TODO: -- Also, how about x-platform testing. e.g. httpd vs apache2


def test_find_entity(patched_entitygen):
    entities = patched_entitygen.entityd_find_entity('Apache', None)
    count = 0
    for entity in entities:
        assert entity.metype == 'Apache'
        if entity.deleted:
            continue
        count += 1
    assert count


def test_find_entity_with_attrs():
    with pytest.raises(LookupError):
        entityd.apacheme.ApacheEntity().entityd_find_entity('Apache', {})


def test_relations(pm, session, kvstore, patched_entitygen): # pylint: disable=unused-argument
    """Apache should have at least one process in relations"""
    procent = entityd.processme.ProcessEntity()
    pm.register(procent,
                name='entityd.processme')
    procent.entityd_sessionstart(session)

    apache = entityd.apacheme.Apache()
    binary = apache.apache_binary
    print(binary) ## getting apachectl...
    processes = procent.entityd_find_entity('Process',
                                            attrs={'binary': binary})
    procs = set(processes)
    for p in procs:
        assert p.metype == 'Process'
        assert p.attrs.get('binary').value == 'apache2'
    entity = next(patched_entitygen.entityd_find_entity('Apache', attrs=None))
    assert len(entity.children._relations) == len(procs)
    assert entity.children._relations == set(p.ueid for p in procs)


@apachectl
def test_config_path(apache):
    path = apache.config_path
    assert os.path.isfile(path)


def test_config_check(entity):
    """Checks the Apache config.

    Currently relies on the system Apache install
    """
    assert entity.attrs.get('config_ok').value in [True, False]


def test_config_check_fails(apache, tmpdir):
    # The check goes via the apachectl binary - how to patch?
    #
    path = tmpdir.join('apache.conf')

    with open(str(path), 'w') as f:
        f.write('-ServerName localhost\n')

    assert apache.check_config(str(path)) is False


def test_rhel():
    # TODO: How do we run a test that works for httpd (on rhel)?
    pass


def test_config_last_modified(apache, tmpdir, monkeypatch):
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


def test_sites_enabled(entity):
    assert isinstance(entity.attrs.get('SitesEnabled').value, list)


def test_get_all_includes(apache):
    conf = apache.config_path
    includes = entityd.apacheme._find_all_includes(conf)
    assert b'/etc/apache2/sites-enabled/000-default.conf' in includes


def test_last_mod(entity):
    assert isinstance(entity.attrs.get('config_last_mod').value, float)
