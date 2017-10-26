import copy
import hashlib
import unittest.mock

import docker.models.images
import pytest

import entityd
import entityd.docker
import entityd.docker.client
import entityd.docker.image


@pytest.fixture
def plugin(pm):
    """A DockerImage instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.
    """
    plugin = entityd.docker.image.DockerImage()
    pm.register(plugin, 'entityd.docker.image.DockerImage')
    return plugin


@pytest.fixture
def image():
    """Full image resource as returned by Docker API."""
    return docker.models.images.Image({
        'Id': ('sha256:c7e11c9e1508ded0e2ece8124e'
               '8f1bb7ddb5582af4a28f14a0139c576e26fc44'),
        'RepoTags': [],
        'RepoDigests': [],
        'Parent': ('sha256:715a7b8803a0564257e7460ee729'
                   'b61c260974eac7cb3281ab7cafeb72b5f25b'),
        'Comment': '',
        'Created': '2017-08-17T12:15:19.260688197Z',
        'Container': ('3d37e345966d95698d68fe6c3baf4b4'
                      '20be02686e2aa085efb612560b2502102'),
        'ContainerConfig': {
            'Hostname': '24a7fbf3d8a5',
            'Domainname': '',
            'User': '',
            'AttachStdin': False,
            'AttachStdout': False,
            'AttachStderr': False,
            'Tty': False,
            'OpenStdin': False,
            'StdinOnce': False,
            'Env': [
                ('PATH=/usr/local/sbin:'
                 '/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'),
            ],
            'Cmd': [
                '/bin/sh',
                '-c',
                ('#(nop) COPY file:d4597a840d088c157bdd6c78a3555944'
                 'f60450c804ffd0a2022ab03cf0c8dfb8 in /srv/nginx/start.sh '),
            ],
            'Image': ('sha256:715a7b8803a0564257e7460ee72'
                      '9b61c260974eac7cb3281ab7cafeb72b5f25b'),
            'Volumes': None,
            'WorkingDir': '',
            'Entrypoint': None,
            'OnBuild': [],
            'Labels': {}
        },
        'DockerVersion': '17.05.0-ce',
        'Author': '',
        'Config': {
            'Hostname': '24a7fbf3d8a5',
            'Domainname': '',
            'User': '',
            'AttachStdin': False,
            'AttachStdout': False,
            'AttachStderr': False,
            'Tty': False,
            'OpenStdin': False,
            'StdinOnce': False,
            'Env': [
                ('PATH=/usr/local/sbin:'
                 '/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'),
            ],
            'Cmd': [
                '/bin/bash',
            ],
            'Image': ('sha256:715a7b8803a0564257e7460ee72'
                      '9b61c260974eac7cb3281ab7cafeb72b5f25b'),
            'Volumes': None,
            'WorkingDir': '',
            'Entrypoint': None,
            'OnBuild': [],
            'Labels': {}
        },
        'Architecture': 'amd64',
        'Os': 'linux',
        'Size': 189430632,
        'VirtualSize': 189430632,
        'GraphDriver': {
            'Data': None,
            'Name': 'aufs',
        },
        'RootFS': {
            'Type': 'layers',
            'Layers': [
                ('sha256:54fe19ca2ee7fe34e45a3cefddd'
                 'e96a95af268c79c976c63619d5e29dc5668a1'),
                ('sha256:5f70bf18a086007016e948b04ae'
                 'd3b82103a36bea41755b6cddfaf10ace3c6ef'),
                ('sha256:a578ad472e857df22c350a84894'
                 'b56e77e3b1630229d3181315b476172103fc3'),
                ('sha256:5b9ed4e67a1fec03fc12f806582'
                 '6bc1e9f5168338109d3e8a5a938778f30c946'),
                ('sha256:0c26e935adc89b9c64e618cad35'
                 '36435330d8c2fda0b0412d2474ef89cccd2aa'),
                ('sha256:4c748d7eccf2dac936b68c53aab'
                 '7deb676b630399144b07c62ed64f8abed7257'),
            ]
        }
    })


@pytest.fixture
def images(plugin, image):
    """Create and configure the plugin with two images."""
    images = []
    for index in range(2):
        hash_ = hashlib.sha256(str(index).encode())
        image_copy = copy.deepcopy(image)
        image_copy.attrs['Id'] = 'sha256:' + hash_.hexdigest()
        plugin._images[image_copy.id] = image
        images.append(image_copy)
    return images


class TestGenerateImages:

    @pytest.mark.parametrize(('tags', 'dangling'), [
        ([], True),
        (['repo/name:latest'], False),
        (['repo/name:latest', 'repo/name:0.1.0'], False),
    ])
    def test(self, monkeypatch, plugin, image, tags, dangling):
        monkeypatch.setattr(
            plugin,
            '_image_label',
            unittest.mock.Mock(wraps=plugin._image_label),
        )
        monkeypatch.setattr(
            plugin,
            '_image_parents',
            unittest.mock.Mock(wraps=plugin._image_parents),
        )
        # Pedantic check that _generate_images returns multiple
        # updates. We don't actually care about the second one.
        image.attrs['RepoTags'] = tags
        plugin._images = {image.id: image, '_': image}
        updates = list(plugin._generate_images())
        assert len(updates) == 2
        update, _ = updates
        assert update.metype == 'Docker:Image'
        assert update.attrs.get('digest').value == (
            'sha256:c7e11c9e1508ded0e2ece8124e'
            '8f1bb7ddb5582af4a28f14a0139c576e26fc44'
        )
        assert update.attrs.get('digest').traits == {'entity:id'}
        assert update.attrs.get('created').value \
            == '2017-08-17T12:15:19.260688197Z'
        assert update.attrs.get('created').traits == {'time:rfc3339'}
        assert update.attrs.get('size').value == 189430632
        assert update.attrs.get('size').traits == {'unit:bytes'}
        assert update.attrs.get('size:virtual').value == 189430632
        assert update.attrs.get('size:virtual').traits == {'unit:bytes'}
        assert update.attrs.get('architecture').value == 'amd64'
        assert update.attrs.get('architecture').traits == set()
        assert update.attrs.get('operating-system').value == 'linux'
        assert update.attrs.get('operating-system').traits == set()
        assert update.attrs.get('docker:version').value == '17.05.0-ce'
        assert update.attrs.get('docker:version').traits == set()
        assert update.attrs.get('docker:driver').value == 'aufs'
        assert update.attrs.get('docker:driver').traits == set()
        assert update.attrs.get('entry-point').value is None
        assert update.attrs.get('entry-point').traits == set()
        assert update.attrs.get('command').value == ['/bin/bash']
        assert update.attrs.get('command').traits == set()
        assert update.attrs.get('dangling').value is dangling
        assert plugin._image_label.call_count == 2
        for args, kwargs in plugin._image_label.call_args_list:
            assert args[0] in updates
            assert args[1] is image
            assert kwargs == {}
        assert plugin._image_parents.call_count == 2
        for args, kwargs in plugin._image_parents.call_args_list:
            assert args[0] in updates
            assert args[1] is image
            assert kwargs == {}


class TestGenerateLabels:

    def test(self, plugin, image):
        image.attrs['Config']['Labels'] = {
            'foo': 'bar',
            'spam': 'eggs',
        }
        plugin._images = {image.id: image}
        labels = {label.label: label for label in plugin._generate_labels()}
        assert len(labels) == 2
        assert labels['foo = bar'].metype == 'Group'
        assert labels['foo = bar'].label == 'foo = bar'  # sanity check
        assert labels['foo = bar'].attrs.get('kind').value == 'label:foo'
        assert labels['foo = bar'].attrs.get('kind').traits == {'entity:id'}
        assert labels['foo = bar'].attrs.get('id').value == 'bar'
        assert labels['foo = bar'].attrs.get('id').traits == {'entity:id'}
        assert len(labels['foo = bar'].parents) == 0
        assert len(labels['foo = bar'].children) == 1
        assert plugin.get_ueid(image.id) in labels['foo = bar'].children
        assert labels['spam = eggs'].metype == 'Group'
        assert labels['spam = eggs'].label == 'spam = eggs'  # sanity check
        assert labels['spam = eggs'].attrs.get('kind').value == 'label:spam'
        assert labels['spam = eggs'].attrs.get('kind').traits == {'entity:id'}
        assert labels['spam = eggs'].attrs.get('id').value == 'eggs'
        assert labels['spam = eggs'].attrs.get('id').traits == {'entity:id'}
        assert len(labels['spam = eggs'].parents) == 0
        assert len(labels['spam = eggs'].children) == 1
        assert plugin.get_ueid(image.id) in labels['spam = eggs'].children

    def test_shared_label(self, plugin, images):
        image_1, image_2 = images
        plugin._images = {image_1.id: image_1, image_2.id: image_2}  # redundant?
        image_1.attrs['Config']['Labels'] = {'foo': 'bar'}
        image_2.attrs['Config']['Labels'] = {'foo': 'bar'}
        labels = {label.label: label for label in plugin._generate_labels()}
        assert len(labels) == 1
        assert labels['foo = bar'].metype == 'Group'
        assert labels['foo = bar'].label == 'foo = bar'  # sanity check
        assert labels['foo = bar'].attrs.get('kind').value == 'label:foo'
        assert labels['foo = bar'].attrs.get('kind').traits == {'entity:id'}
        assert labels['foo = bar'].attrs.get('id').value == 'bar'
        assert labels['foo = bar'].attrs.get('id').traits == {'entity:id'}
        assert len(labels['foo = bar'].parents) == 0
        assert len(labels['foo = bar'].children) == 2
        assert plugin.get_ueid(image_1.id) in labels['foo = bar'].children
        assert plugin.get_ueid(image_2.id) in labels['foo = bar'].children


class TestImageParents:

    def test_parent(self, plugin, images):
        image, image_parent = images
        image.attrs['Parent']= image_parent.attrs['Id']
        update = entityd.EntityUpdate('Test')
        plugin._image_parents(update, image)
        assert len(update.parents) == 1
        assert len(update.children) == 0
        assert plugin.get_ueid(image_parent.id) in update.parents

    def test_parent_foreign(self, plugin, images):
        image, _ = images
        image_parent_digest = 'sha256:' + hashlib.sha256(b'').hexdigest()
        image.attrs['Parent'] = image_parent_digest
        update = entityd.EntityUpdate('Test')
        plugin._image_parents(update, image)
        assert len(update.parents) == 1
        assert len(update.children) == 0
        assert plugin.get_ueid(image_parent_digest) in update.parents


class TestGetUEID:

    def test_instance(self, plugin, image):
        assert str(plugin.get_ueid(image.id)) \
            == '169def0c5c9300308158edd551d80358'

    def test_class(self, plugin, image):
        assert str(plugin.__class__.get_ueid(image.id)) \
            == '169def0c5c9300308158edd551d80358'

    def test_helper(self, image):
        assert str(entityd.docker.get_ueid('DockerImage', image.id)) \
            == '169def0c5c9300308158edd551d80358'


class TestImageLabel:

    def test_id(self, plugin):
        update = entityd.EntityUpdate('Test')
        image = docker.models.images.Image({
            'Id': ('sha256:abc123def45600000000000'
                   '00000000000000000000000000000000000000000'),
            'RepoTags': [],
        })
        plugin._image_label(update, image)
        assert update.label == 'abc123def456'

    def test_tag(self, plugin):
        update = entityd.EntityUpdate('Test')
        image = docker.models.images.Image({
            'Id': ('sha256:abc123def45600000000000'
                   '00000000000000000000000000000000000000000'),
            'RepoTags': [
                'repo/name:0.1.0',
            ],
        })
        plugin._image_label(update, image)
        assert update.label == 'repo/name:0.1.0'

    def test_tag_multiple(self, plugin):
        update = entityd.EntityUpdate('Test')
        image = docker.models.images.Image({
            'Id': ('sha256:abc123def45600000000000'
                   '00000000000000000000000000000000000000000'),
            'RepoTags': [
                'repo/name:0.1.0',
                'repo/name:0.2.0',
            ],
        })
        plugin._image_label(update, image)
        assert update.label == 'repo/name:0.2.0'

    def test_latest(self, plugin):
        update = entityd.EntityUpdate('Test')
        image = docker.models.images.Image({
            'Id': ('sha256:abc123def45600000000000'
                   '00000000000000000000000000000000000000000'),
            'RepoTags': [
                'repo/name:latest',
            ],
        })
        plugin._image_label(update, image)
        assert update.label == 'repo/name:latest'

    def test_latest_multiple(self, plugin):
        update = entityd.EntityUpdate('Test')
        image = docker.models.images.Image({
            'Id': ('sha256:abc123def45600000000000'
                   '00000000000000000000000000000000000000000'),
            'RepoTags': [
                'a/repo/name:latest',
                'b/repo/name:latest',
            ],
        })
        plugin._image_label(update, image)
        assert update.label == 'b/repo/name:latest'

    def test_latest_multiple_mixed(self, plugin):
        update = entityd.EntityUpdate('Test')
        image = docker.models.images.Image({
            'Id': ('sha256:abc123def45600000000000'
                   '00000000000000000000000000000000000000000'),
            'RepoTags': [
                'repo/name:0.1.0',
                'repo/name:latest',
            ],
        })
        plugin._image_label(update, image)
        assert update.label == 'repo/name:0.1.0'


class TestConfigure:

    def test(self, config, plugin):
        plugin.entityd_configure(config)
        assert set(config.entities.keys()) == {
            'Docker:Image',
            'Group',
        }
        assert len(config.entities['Docker:Image']) == 1
        assert len(config.entities['Group']) == 1
        assert list(config.entities['Docker:Image'])[0].name \
            == 'entityd.docker.image.DockerImage'
        assert list(config.entities['Group'])[0].name \
            == 'entityd.docker.image.DockerImage'


class TestCollectionBefore:

    @pytest.fixture
    def client(self, monkeypatch, images):
        """Replace Docker client with a mock.

        The client is configured to list two fake Docker images.
        """
        monkeypatch.setattr(
            entityd.docker.client.DockerClient,
            'get_client',
            unittest.mock.Mock(),
        )
        client = entityd.docker.client.DockerClient.get_client.return_value
        client.images.list.return_value = images
        return client

    def test(self, session, plugin, images, client):
        plugin._images = {}
        plugin.entityd_collection_before(session)
        assert len(plugin._images) == 2
        assert plugin._images[images[0].id] is images[0]
        assert plugin._images[images[1].id] is images[1]

    def test_unavailable(self, monkeypatch, session, plugin):
        monkeypatch.setattr(
            entityd.docker.client.DockerClient,
            'client_available',
            unittest.mock.Mock(return_value=False),
        )
        assert plugin._images == {}
        plugin.entityd_collection_before(session)
        assert plugin._images == {}


class TestCollectionAfter:

    def test(self, session, plugin, image):
        plugin._images = {image.id: image}
        plugin.entityd_collection_after(session, ())
        assert plugin._images == {}


class TestFindEntity:

    @pytest.mark.parametrize('type_', ['Docker:Image', 'Group'])
    def test(self, monkeypatch, plugin, type_):
        generator_function = unittest.mock.Mock()
        monkeypatch.setattr(plugin, '_generate_images', generator_function)
        monkeypatch.setattr(plugin, '_generate_labels', generator_function)
        generator = plugin.entityd_find_entity(type_)
        assert generator is generator_function.return_value
        assert generator_function.call_count == 1
        assert generator_function.call_args[0] == ()
        assert generator_function.call_args[1] == {}

    @pytest.mark.parametrize('type_', ['Docker:Image', 'Group'])
    def test_filtering(self, plugin, type_):
        with pytest.raises(LookupError) as exception:
            plugin.entityd_find_entity(type_, attrs={'foo': 'bar'})
        assert 'filtering not supported' in str(exception.value)

    def test_type_not_implemented(self, plugin):
        assert plugin.entityd_find_entity('Foo') is None
