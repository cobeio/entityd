import os
import subprocess
import time

import pytest
import requests

import entityd.apacheme
import entityd.core
import entityd.hostme
import entityd.processme


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
 -D HTTPD_ROOT="/etc/apache2"
 -D SUEXEC_BIN="/usr/lib/apache2/suexec"
 -D DEFAULT_PIDLOG="/var/run/apache2.pid"
 -D DEFAULT_SCOREBOARD="logs/apache_runtime_status"
 -D DEFAULT_ERRORLOG="logs/error_log"
 -D AP_TYPES_CONFIG_FILE="mime.types"
 -D SERVER_CONFIG_FILE="apache2.conf"
"""


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
    gen.apache._version = 'Apache/2.4.7 (Ubuntu)'
    gen.apache._config_path = '/etc/apache2/apache2.conf'
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
        assert 'Apache/2' in entity.attrs.get('version').value
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
        assert 'Apache/2' in entity.attrs.get('version').value
        assert os.path.isfile(entity.attrs.get('config_path').value)
        count += 1
    assert count


def test_find_entity_no_apache_running(patched_entitygen, monkeypatch):
    monkeypatch.setattr(entityd.apacheme, '_get_apache_status',
                        pytest.Mock(side_effect=RuntimeError))
    gen = patched_entitygen.entityd_find_entity('Apache', None)
    assert list(gen) == []


def test_find_entity_no_apache_installed(patched_entitygen, monkeypatch):
    patched_entitygen.apache._apachectl_binary = None
    monkeypatch.setattr(entityd.apacheme, '_apachectl_binary',
                        pytest.Mock(side_effect=RuntimeError))
    gen = patched_entitygen.entityd_find_entity('Apache', None)
    assert list(gen) == []


def test_find_entity_with_attrs():
    with pytest.raises(LookupError):
        entityd.apacheme.ApacheEntity().entityd_find_entity('Apache', {})


def test_entity_deleted_installed(patched_entitygen, monkeypatch):
    gen = patched_entitygen.entityd_find_entity('Apache', None)
    last_entity = next(gen)
    last_ueid = last_entity.ueid
    monkeypatch.setattr(entityd.apacheme, '_apachectl_binary',
                        pytest.Mock(side_effect=RuntimeError))
    patched_entitygen.apache._apachectl_binary = None
    gen = patched_entitygen.entityd_find_entity('Apache', None)
    entity = next(gen)
    assert entity.deleted
    assert entity.ueid == last_ueid


def test_entity_deleted_running(patched_entitygen, monkeypatch):
    gen = patched_entitygen.entityd_find_entity('Apache', None)
    last_entity = next(gen)
    last_ueid = last_entity.ueid
    monkeypatch.setattr(entityd.apacheme, '_get_apache_status',
                        pytest.Mock(side_effect=RuntimeError))
    gen = patched_entitygen.entityd_find_entity('Apache', None)
    entity = next(gen)
    assert entity.deleted
    assert entity.ueid == last_ueid


def test_relations(pm, session, kvstore, patched_entitygen):  # pylint: disable=unused-argument
    gen = patched_entitygen
    procent = entityd.processme.ProcessEntity()
    pm.register(procent,
                name='entityd.processme')
    procent.entityd_sessionstart(session)

    hostgen = entityd.hostme.HostEntity()
    pm.register(hostgen, name='entityd.hostme')
    hostgen.entityd_sessionstart(session)
    hosts = hostgen.entityd_find_entity('Host', None)

    # Use py.test as a binary so we're not dependent on apache running.
    gen.apache._apache_binary = 'py.test'
    processes = procent.entityd_find_entity(
        'Process', attrs={'binary': 'py.test'})
    procs = set(processes)
    for p in procs:
        assert p.metype == 'Process'
        assert p.attrs.get('binary').value == 'py.test'

    entity = next(gen.entityd_find_entity('Apache', attrs=None))
    assert len(entity.children._relations) == len(procs)
    assert entity.children._relations == set(p.ueid for p in procs)

    assert len(entity.parents._relations) == 1
    assert entity.parents._relations == set(host.ueid for host in hosts)


def test_config_path_from_file(apache, monkeypatch):
    monkeypatch.setattr(subprocess, 'check_output',
                        pytest.Mock(return_value=APACHECTL__V))
    assert apache.config_path == '/etc/apache2/apache2.conf'


def test_config_path(apache, monkeypatch):
    apache._apachectl_binary = 'apachectl'
    monkeypatch.setattr(entityd.apacheme, '_apache_config',
                        pytest.Mock(return_value='/etc/apache2/apache2.conf'))
    path = apache.config_path
    assert path == '/etc/apache2/apache2.conf'


def test_config_path_fails(monkeypatch):
    monkeypatch.setattr(subprocess, 'check_output', pytest.Mock(
        return_value='no config file here'))
    with pytest.raises(RuntimeError):
        entityd.apacheme._apache_config('httpd')


def test_config_check(apache, monkeypatch):
    apache._apachectl_binary = 'apachectl'
    apache._config_path = '/etc/apache2/apache2.conf'
    monkeypatch.setattr(subprocess, 'check_call',
                        pytest.Mock(return_value=0))
    assert apache.check_config() is True


def test_config_check_fails(apache, monkeypatch):
    apache._apachectl_binary = 'apachectl'
    apache._config_path = '/etc/apache2/apache2.conf'
    monkeypatch.setattr(subprocess, 'check_call',
                        pytest.Mock(return_value=-1))

    assert apache.check_config() is False

    monkeypatch.setattr(
        subprocess, 'check_call',
        pytest.Mock(side_effect=subprocess.CalledProcessError(-1, '')))
    assert apache.check_config() is False


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


def test_version(apache, monkeypatch):
    monkeypatch.setattr(subprocess, 'check_output',
                        pytest.Mock(return_value=APACHECTL__V))
    apache._apachectl_binary = 'apachectl'
    assert apache.version == 'Apache/2.4.7 (Ubuntu)'


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
    includes = entityd.apacheme._find_all_includes(str(conf))
    assert str(site) in includes
