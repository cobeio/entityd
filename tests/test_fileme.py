import re

import pytest

import entityd.fileme


@pytest.fixture
def fileent(pm, host_entity_plugin):  # pylint: disable=unused-argument
    """A entityd.fileme.FileEntity instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.

    """
    fileent = entityd.fileme.FileEntity()
    pm.register(fileent, 'entityd.fileme.FileEntity')
    return fileent


def test_configure(fileent, config):
    fileent.entityd_configure(config)
    assert config.entities['File'].obj is fileent


def test_find_entity(tmpdir, session, fileent):
    fileent.entityd_sessionstart(session)
    file = tmpdir.join('testfile.txt')
    with file.open('w') as f:
        f.write('test line\n')

    entities = fileent.entityd_find_entity('File', attrs={'path': str(file)})
    entity = next(entities)
    assert entity.attrs.get('path').value == str(file)
    assert isinstance(entity.attrs.get('size').value, int)
    assert entity.attrs.get('size').traits == {'metric:gauge', 'unit:bytes'}


def test_calling_with_no_attrs(fileent):
    result = fileent.entityd_find_entity('File', None)
    with pytest.raises(StopIteration):
        print(next(result))


def test_calling_with_non_existent_path(tmpdir, loghandler, session, fileent):
    fileent.entityd_sessionstart(session)
    fileent.entityd_configure(session.config)
    file_ = tmpdir.join('testfile.txt')
    entities = fileent.entityd_find_entity('File', attrs={'path': str(file_)})
    with pytest.raises(StopIteration):
        next(entities)
    assert loghandler.has_debug(re.compile(r'Failed to create entity'))
    assert loghandler.has_debug(re.compile(str(file_)))
