import os
import tempfile

import pytest

import entityd.fileme
import entityd.hostme
import entityd.mysqlme
import entityd.processme


@pytest.fixture
def procent(request, pm, session, host_entity_plugin, monkeypatch):  # pylint: disable=unused-argument
    procent = entityd.processme.ProcessEntity()
    proc = entityd.EntityUpdate('Process')
    proc.attrs.set('pid', 123)
    proc.attrs.set('ppid', 0)
    proc.attrs.set('binary', 'mysqld')
    proc.attrs.set('command', 'mysqld')
    pm.register(
        procent, name='entityd.processme')
    monkeypatch.setattr(
        procent, 'filtered_processes', pytest.Mock(return_value=[proc]))
    procent.entityd_sessionstart(session)
    request.addfinalizer(procent.entityd_sessionfinish)
    return procent


@pytest.fixture
def mock_config_path(request, monkeypatch):
    temp = tempfile.NamedTemporaryFile(delete=True)
    monkeypatch.setattr(entityd.mysqlme.MySQL,
                        'config_path',
                        pytest.Mock(return_value=temp.name))
    request.addfinalizer(lambda: temp.close())
    return entityd.mysqlme.MySQL.config_path


@pytest.fixture
def mock_mysql(mock_config_path, pm, config, session, procent):  # pylint: disable=unused-argument
    mysql = entityd.mysqlme.MySQLEntity()
    pm.register(
        mysql, name='entityd.mysqlme.MySQLEntity')
    mysql.entityd_sessionstart(session)
    mysql.entityd_configure(config)
    return mysql


def test_get_entities(mock_mysql):
    entities = mock_mysql.entityd_find_entity(
        name='MySQL', attrs=None, include_ondemand=False)
    entity = next(entities)
    assert entity.metype == 'MySQL'
    assert entity.attrs.get('process_id').value == 123


def test_mysql_process_but_no_files_with_log(monkeypatch,
                                             mock_mysql, loghandler):
    # This covers situation of entityd running in container
    def config_path_mock(self):  # pylint: disable=unused-argument
        raise entityd.mysqlme.MySQLNotFoundError()
    monkeypatch.setattr(entityd.mysqlme.MySQL,
                        'config_path', config_path_mock)
    assert mock_mysql._log_flag is False
    entities = mock_mysql.entityd_find_entity(
        name='MySQL', attrs=None, include_ondemand=True)
    entities = list(entities)
    assert len(entities) == 0
    assert loghandler.has_warning()
    assert mock_mysql._log_flag is True


def test_mysql_process_but_no_files_no_log(monkeypatch,
                                           mock_mysql, loghandler):
    def config_path_mock(self):  # pylint: disable=unused-argument
        raise entityd.mysqlme.MySQLNotFoundError()
    monkeypatch.setattr(entityd.mysqlme.MySQL,
                        'config_path', config_path_mock)
    mock_mysql._log_flag = True
    entities = mock_mysql.entityd_find_entity(
        name='MySQL', attrs=None, include_ondemand=True)
    entities = list(entities)
    assert len(entities) == 0
    assert not loghandler.has_warning()
    assert mock_mysql._log_flag is True


def test_multiple_processes(monkeypatch, procent, mock_mysql):
    p1 = entityd.EntityUpdate('Process')
    p1.attrs.set('pid', 123)
    p1.attrs.set('ppid', 0)
    p1.attrs.set('binary', 'mysqld')
    p1.attrs.set('command', 'mysqld')
    p2 = entityd.EntityUpdate('Process')
    p2.attrs.set('pid', 456)
    p2.attrs.set('ppid', 123)
    p2.attrs.set('binary', 'mysqld')
    p2.attrs.set('command', 'mysqld')
    p3 = entityd.EntityUpdate('Process')
    p3.attrs.set('pid', 789)
    p3.attrs.set('ppid', 0)
    p3.attrs.set('binary', 'mysqld')
    p3.attrs.set('command', 'mysqld')
    monkeypatch.setattr(
        procent, 'filtered_processes', pytest.Mock(return_value=[p1, p2, p3]))
    entities = mock_mysql.entityd_find_entity(
        name='MySQL', attrs=None, include_ondemand=False)
    pids = sorted(e.attrs.get('process_id').value for e in entities)
    assert pids == [123, 789]


def test_attrs_must_be_none(mock_mysql):
    with pytest.raises(LookupError):
        mock_mysql.entityd_find_entity(
            name='MySQL', attrs={'not': None}, include_ondemand=False)


def test_host_stored_and_returned(pm, session, kvstore, mock_mysql):  # pylint: disable=unused-argument
    hostgen = entityd.hostme.HostEntity()
    pm.register(hostgen, name='entityd.hostme')
    hostgen.entityd_sessionstart(session)

    entities = mock_mysql.entityd_find_entity(
        name='MySQL', attrs=None, include_ondemand=False)
    next(entities)
    ueid = mock_mysql._host_ueid
    assert ueid
    entities = mock_mysql.entityd_find_entity(
        name='MySQL', attrs=None, include_ondemand=False)
    entity = next(entities)
    assert ueid is mock_mysql._host_ueid
    assert entity.attrs.get('host').value == str(ueid)
    assert ueid in entity.parents._relations


def test_config_file(pm, session, mock_mysql):
    filegen = entityd.fileme.FileEntity()
    pm.register(filegen, 'entityd.fileme.FileEntity')
    filegen.entityd_sessionstart(session)

    entities = mock_mysql.entityd_find_entity(name='MySQL', attrs=None,
                                              include_ondemand=True)
    file, mysql = sorted(entities, key=lambda e: e.metype)
    assert file.ueid in mysql.children._relations


@pytest.mark.parametrize('path', ['/etc/my.cnf',
                                  '/etc/mysql/my.cnf',
                                  '/usr/etc/my.cnf',
                                  os.path.expanduser('~/.my.cnf')])
def test_config_path_defaults(monkeypatch, path):
    """MySQL should use the first file that exists from a given list."""

    def isfile(test_path):
        return test_path == path

    proc = entityd.EntityUpdate('Process')
    proc.attrs.set('command', 'mysqld')
    mysql = entityd.mysqlme.MySQL(proc)
    monkeypatch.setattr(entityd.mysqlme.os.path, 'isfile', isfile)
    assert mysql.config_path() == path


def test_config_path_not_found(monkeypatch):
    monkeypatch.setattr(entityd.mysqlme.os.path,
                        'isfile', pytest.Mock(return_value=False))
    proc = entityd.EntityUpdate('Process')
    proc.attrs.set('command', 'mysqld')
    mysql = entityd.mysqlme.MySQL(proc)
    with pytest.raises(entityd.mysqlme.MySQLNotFoundError):
        mysql.config_path()


@pytest.mark.parametrize('command, path', [
    ('--defaults-file=/path/to/my.cnf', '/path/to/my.cnf'),
    ('--defaults-file /path/2/my.cnf', '/path/2/my.cnf'),
])
def test_config_pathoverride(command, path):
    proc = entityd.EntityUpdate('Process')
    proc.attrs.set('command', command)
    mysql = entityd.mysqlme.MySQL(proc)
    assert mysql.config_path() == path
