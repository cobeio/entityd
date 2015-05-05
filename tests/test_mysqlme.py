import entityd.fileme
import entityd.hostme
import entityd.mysqlme
import entityd.processme

import pytest


@pytest.fixture
def procent(pm, session, monkeypatch):
    procent = entityd.processme.ProcessEntity()
    proc = entityd.EntityUpdate('Process')
    proc.attrs.set('pid', 123)
    proc.attrs.set('ppid', 0)
    proc.attrs.set('binary', 'mysqld')
    pm.register(procent,
                name='entityd.processme')
    monkeypatch.setattr(procent,
                        'filtered_processes',
                        pytest.Mock(return_value=[proc]))
    procent.entityd_sessionstart(session)
    return procent


@pytest.fixture
def mock_mysql(pm, config, session, procent):  # pylint: disable=unused-argument
    mysql = entityd.mysqlme.MySQLEntity()
    pm.register(mysql,
                name='entityd.mysqlme.MySQLEntity')
    mysql.entityd_sessionstart(session)
    mysql.entityd_configure(config)
    return mysql


def test_get_entities(mock_mysql):
    entities = mock_mysql.entityd_find_entity(name='MySQL', attrs=None, include_ondemand=False)
    entity = next(entities)
    assert entity.metype == 'MySQL'
    assert entity.attrs.get('process_id').value == 123


def test_multiple_processes(monkeypatch, procent, mock_mysql):
    p1 = entityd.EntityUpdate('Process')
    p1.attrs.set('pid', 123)
    p1.attrs.set('ppid', 0)
    p1.attrs.set('binary', 'mysqld')
    p2 = entityd.EntityUpdate('Process')
    p2.attrs.set('pid', 456)
    p2.attrs.set('ppid', 123)
    p2.attrs.set('binary', 'mysqld')
    p3 = entityd.EntityUpdate('Process')
    p3.attrs.set('pid', 789)
    p3.attrs.set('ppid', 0)
    p3.attrs.set('binary', 'mysqld')
    monkeypatch.setattr(procent, 'filtered_processes',
                        pytest.Mock(return_value=[p1, p2, p3]))
    entities = mock_mysql.entityd_find_entity(name='MySQL', attrs=None, include_ondemand=False)
    assert sorted(e.attrs.get('process_id').value for e in entities) == [123, 789]


def test_attrs_must_be_none(mock_mysql):
    with pytest.raises(LookupError):
        mock_mysql.entityd_find_entity(name='MySQL', attrs={'not': None},
                                       include_ondemand=False)


def test_host_stored_and_returned(pm, session, kvstore, mock_mysql):  # pylint: disable=unused-argument
    hostgen = entityd.hostme.HostEntity()
    pm.register(hostgen, name='entityd.hostme')
    hostgen.entityd_sessionstart(session)

    entities = mock_mysql.entityd_find_entity(name='MySQL', attrs=None, include_ondemand=False)
    next(entities)
    ueid = mock_mysql._host_ueid
    assert ueid
    entities = mock_mysql.entityd_find_entity(name='MySQL', attrs=None, include_ondemand=False)
    entity = next(entities)
    assert ueid is mock_mysql._host_ueid
    assert entity.attrs.get('host').value == ueid
    assert ueid in entity.parents._relations


def test_config_file(pm, session, mock_mysql):
    filegen = entityd.fileme.FileEntity()
    pm.register(filegen, 'entityd.fileme.FileEntity')
    filegen.entityd_sessionstart(session)

    entities = mock_mysql.entityd_find_entity(name='MySQL', attrs=None, include_ondemand=True)
    file, mysql = sorted(entities, key=lambda e: e.metype)
    assert file.ueid in mysql.children._relations
