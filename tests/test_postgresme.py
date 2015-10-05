import entityd.fileme
import entityd.hostme
import entityd.postgresme
import entityd.processme

import pytest
import os


@pytest.fixture
def procent(pm, session, monkeypatch):
    procent = entityd.processme.ProcessEntity()
    proc = entityd.EntityUpdate('Process')
    proc.attrs.set('pid', 123)
    proc.attrs.set('ppid', 0)
    proc.attrs.set('binary', 'postgres')
    proc.attrs.set('command', 'postgres')
    pm.register(procent,
                name='entityd.processme')
    monkeypatch.setattr(procent,
                        'filtered_processes',
                        pytest.Mock(return_value=[proc]))
    procent.entityd_sessionstart(session)
    return procent


@pytest.fixture
def create_temp_pgconf(request):
    """ Ensure file postgresql.conf for testing on m/c with no postgreSQL"""
    if os.path.isfile(os.path.expanduser('~/') + 'postgresql.conf'):
        return True
    else:
        pgconf_path = os.path.expanduser('~/')+'postgresql.conf'
        with open(pgconf_path, 'w') as file:
            file.write('')
        def delfile():
            os.remove(pgconf_path)
        request.addfinalizer(delfile)
        return True


@pytest.fixture
def mock_postgres(create_temp_pgconf, pm, config, session, procent):  # pylint: disable=unused-argument
    postgres = entityd.postgresme.PostgreSQLEntity()
    pm.register(postgres,
                name='entityd.postgresme.PostgreSQLEntity')
    postgres.entityd_sessionstart(session)
    postgres.entityd_configure(config)
    return postgres


def test_get_entities(mock_postgres):
    entities = mock_postgres.entityd_find_entity(name='PostgreSQL', attrs=None,
                                              include_ondemand=False)
    entity = next(entities)
    assert entity.metype == 'PostgreSQL'
    assert entity.attrs.get('process_id').value == 123


def test_multiple_processes(monkeypatch, procent, mock_postgres):
    p1 = entityd.EntityUpdate('Process')
    p1.attrs.set('pid', 123)
    p1.attrs.set('ppid', 0)
    p1.attrs.set('binary', 'postgres')
    p1.attrs.set('command', 'postgres')
    p2 = entityd.EntityUpdate('Process')
    p2.attrs.set('pid', 456)
    p2.attrs.set('ppid', 123)
    p2.attrs.set('binary', 'postgres')
    p2.attrs.set('command', 'postgres')
    p3 = entityd.EntityUpdate('Process')
    p3.attrs.set('pid', 789)
    p3.attrs.set('ppid', 0)
    p3.attrs.set('binary', 'postgres')
    p3.attrs.set('command', 'postgres')
    monkeypatch.setattr(procent, 'filtered_processes',
                        pytest.Mock(return_value=[p1, p2, p3]))
    entities = mock_postgres.entityd_find_entity(name='PostgreSQL', attrs=None,
                                              include_ondemand=False)
    pids = sorted(e.attrs.get('process_id').value for e in entities)
    assert pids == [123, 789]


def test_attrs_must_be_none(mock_postgres):
    with pytest.raises(LookupError):
        mock_postgres.entityd_find_entity(name='PostgreSQL', attrs={'not': None},
                                       include_ondemand=False)


def test_host_stored_and_returned(pm, session, kvstore, mock_postgres):  # pylint: disable=unused-argument
    hostgen = entityd.hostme.HostEntity()
    pm.register(hostgen, name='entityd.hostme')
    hostgen.entityd_sessionstart(session)

    entities = mock_postgres.entityd_find_entity(name='PostgreSQL', attrs=None,
                                              include_ondemand=False)
    next(entities)
    ueid = mock_postgres._host_ueid
    assert ueid
    entities = mock_postgres.entityd_find_entity(name='PostgreSQL', attrs=None,
                                              include_ondemand=False)
    entity = next(entities)
    assert ueid is mock_postgres._host_ueid
    assert entity.attrs.get('host').value == ueid
    assert ueid in entity.parents._relations


def test_config_file(pm, session, mock_postgres):
    filegen = entityd.fileme.FileEntity()
    pm.register(filegen, 'entityd.fileme.FileEntity')
    filegen.entityd_sessionstart(session)

    entities = mock_postgres.entityd_find_entity(name='PostgreSQL', attrs=None,
                                              include_ondemand=True)
    file, postgres = sorted(entities, key=lambda e: e.metype)
    assert file.ueid in postgres.children._relations


@pytest.mark.parametrize('path', ['/var/lib/pgsql/data/postgresql.conf'] + \
                                ['/etc/postgresql/' + str(8.0 + n/10) +
                                '/main/postgresql.conf' for n in range(50)])
def test_config_path_defaults(monkeypatch, path):
    """PostgreSQL should use the first file that exists from a given list"""

    def isfile(test_path):
        if test_path == path:
            return True
        else:
            return False

    proc = entityd.EntityUpdate('Process')
    proc.attrs.set('command', 'postgres')
    postgres = entityd.postgresme.PostgreSQL(proc)
    monkeypatch.setattr(entityd.postgresme.os.path, 'isfile', isfile)
    assert postgres.config_path() == path


def test_config_path_not_found(monkeypatch):
    monkeypatch.setattr(entityd.postgresme.os.path,
                        'isfile', pytest.Mock(return_value=False))
    proc = entityd.EntityUpdate('Process')
    proc.attrs.set('command', 'postgres')
    postgres = entityd.postgresme.PostgreSQL(proc)
    with pytest.raises(entityd.postgresme.PostgreSQLNotFound):
        postgres.config_path()


@pytest.mark.parametrize('command, path', [
    ('-c config_file=/etc/postgresql/9.3/main/postgresql.conf',
     '/etc/postgresql/9.3/main/postgresql.conf'),
    ('-c config_file=/etc/postgresql/8.3/main/postgresql.conf',
     '/etc/postgresql/8.3/main/postgresql.conf'),
])
def test_config_pathoverride(command, path):
    proc = entityd.EntityUpdate('Process')
    proc.attrs.set('command', command)
    postgres = entityd.postgresme.PostgreSQL(proc)
    assert postgres.config_path() == path