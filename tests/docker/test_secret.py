import copy
import unittest.mock

import cobe
import docker.models.secrets
import docker.models.services
import pytest

import entityd
import entityd.docker
import entityd.docker.client
import entityd.docker.swarm


@pytest.fixture
def plugin(pm):
    """A DockerImage instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.
    """
    plugin = entityd.docker.swarm.DockerSecret()
    pm.register(plugin, 'entityd.docker.swarm.DockerSecret')
    return plugin


@pytest.fixture
def secret():
    """Full secret resource as returned by Docker API."""
    return docker.models.secrets.Secret({
        'ID': 'jsr6trjiy665b1df33ctxktiy',
        'Version': {
            'Index': 28,
        },
        'CreatedAt': '2017-09-13T12:18:01.746850629Z',
        'UpdatedAt': '2017-09-13T12:18:30.746850629Z',
        'Spec': {
            'Name': 'cobe-test-key',
            'Labels': {},
        },
    })


@pytest.fixture
def secrets(plugin, secret):
    """Create two distinct secrets."""
    secrets = []
    for index in range(2):
        secret_copy = copy.deepcopy(secret)
        secret_copy.attrs['ID'] = chr(97 + index) * 25
        secrets.append(secret_copy)
    return secrets


@pytest.fixture
def client_info():
    return {
        'Swarm': {
            'Cluster': {
                'CreatedAt': '2017-09-18T11:02:01.903734295Z',
                'ID': 'v1w5dux11fec5252r3hciqgzp',
                'Spec': {
                    'EncryptionConfig': {'AutoLockManagers': False},
                    'Labels': {},
                    'Name': 'default',
                    'Raft': {
                        'ElectionTick': 3,
                        'HeartbeatTick': 1,
                        'KeepOldSnapshots': 0,
                        'LogEntriesForSlowFollowers': 500,
                        'SnapshotInterval': 10000,
                    },
                },
            },
            'ControlAvailable': True,
            'Error': '',
            'LocalNodeState': 'active',
            'Managers': 1,
            'Nodes': 1,
        },
    }


@pytest.fixture
def service(monkeypatch):
    """Service resource as return by Docker API.

    The service object will also have a mocked out ``task`` method
    that returns a single, partial task resource.
    """
    service = docker.models.services.Service({
        'ID': 'ocipr9lmcjdebr6jo64e8nqtf',
        'Version': {
            'Index': 5230,
        },
        'CreatedAt': '2017-09-13T14:14:20.998504199Z',
        'UpdatedAt': '2017-09-13T14:14:21.004402744Z',
        'Spec': {
            'Name': 'cobe-agent-test_node',
            'Labels': {
                'com.docker.stack.image': 'cobeio/entityd:0.29.0',
                'com.docker.stack.namespace': 'cobe-agent-test',
            },
            'TaskTemplate': {
                'ContainerSpec': {
                    'Image': ('cobeio/entityd:0.29.0@sha256:f152e996d0ea9eda10'
                              'a8262f1079682eb41dccbd75fa28ee879d593c1c668eeb'),
                    'Labels': {
                        'com.docker.stack.namespace': 'cobe-agent-test',
                    },
                    'Args': [],
                    'Env': [],
                    'Privileges': {
                        'CredentialSpec': None,
                        'SELinuxContext': None,
                    },
                    'Mounts': [],
                    'StopGracePeriod': 10000000000,
                    'DNSConfig': {},
                    'Secrets': [  # Order matters
                        {
                            'File': {
                                'Name': 'cobe-agent-test-key',
                                'UID': '0',
                                'GID': '0',
                                'Mode': 292,
                            },
                            'SecretID': 'yr3bkkujr0yv4feillj0v2kbn',
                            'SecretName': 'cobe-agent-test-key',
                        },
                        {
                            'File': {
                                'Name': 'cobe-agent-test-key-receiver',
                                'UID': '0',
                                'GID': '0',
                                'Mode': 292,
                            },
                            'SecretID': '5hvkpeo6z2ez17c6r6g9caxy8',
                            'SecretName': 'cobe-agent-test-key-receiver',
                        }
                    ]
                },
                'Resources': {},
                'RestartPolicy': {
                    'Condition': 'any',
                    'Delay': 5000000000,
                    'MaxAttempts': 0,
                },
                'Placement': {
                    'Platforms': [
                        {
                            'Architecture': 'amd64',
                            'OS': 'linux',
                        }
                    ]
                },
                'Networks': [
                    {
                        'Target': '7us1zxq7pvxvklh9ks9ga5ay5',
                        'Aliases': [
                            'node',
                        ]
                    }
                ],
                'ForceUpdate': 0,
                'Runtime': 'container',
            },
            'Mode': {
                'Global': {},
            },
            'UpdateConfig': {
                'Parallelism': 1,
                'FailureAction': 'pause',
                'Monitor': 5000000000,
                'MaxFailureRatio': 0,
                'Order': 'stop-first',
            },
            'RollbackConfig': {
                'Parallelism': 1,
                'FailureAction': 'pause',
                'Monitor': 5000000000,
                'MaxFailureRatio': 0,
                'Order': 'stop-first',
            },
            'EndpointSpec': {
                'Mode': 'vip',
            }
        },
        'Endpoint': {
            'Spec': {
                'Mode': 'vip',
            },
            'VirtualIPs': [
                {
                    'NetworkID': '7us1zxq7pvxvklh9ks9ga5ay5',
                    'Addr': '10.0.0.2/24',
                }
            ]
        }
    })
    monkeypatch.setattr(service, 'tasks', unittest.mock.Mock())
    service.tasks.return_value = [{
        'ID': 'vkwz03esqgvn3i2yyz33i3s8p',
        'Labels': {},
        'Status': {
            'ContainerStatus': {
                'ContainerID': ('adab79cca4e176592db386a10d8b9'
                                '64fecd4c8a3c4b5707e6b217598adeef802'),
            },
            'Message': 'finished',
            'PortStatus': {},
            'State': 'complete',
            'Timestamp': '2017-09-18T08:19:33.51080512Z',
        },
    }]
    return service


class TestGenerateSecrets:

    def test(self, plugin, secret):
        # Pedantic check that _generate_secrets returns multiple
        # updates. We don't actually care about the second one.
        plugin._swarm_ueid = cobe.UEID('86063fb5b7e8a5ae9ef249972f656b18')
        plugin._secrets = {secret.id: secret, '_': secret}
        updates = list(plugin._generate_secrets())
        assert len(updates) == 2
        update, _ = updates
        assert update.metype == 'Docker:Secret'
        assert update.label == 'cobe-test-key'
        assert update.attrs.get('id').value == 'jsr6trjiy665b1df33ctxktiy'
        assert update.attrs.get('id').traits == {'entity:id'}
        assert update.attrs.get('name').value == 'cobe-test-key'
        assert update.attrs.get('name').traits == set()
        assert update.attrs.get('created').value \
            == '2017-09-13T12:18:01.746850629Z'
        assert update.attrs.get('created').traits == {'time:rfc3339'}
        assert update.attrs.get('updated').value \
            == '2017-09-13T12:18:30.746850629Z'
        assert update.attrs.get('updated').traits == {'time:rfc3339'}
        assert len(update.parents) == 1
        assert len(update.children) == 0
        assert cobe.UEID('86063fb5b7e8a5ae9ef249972f656b18') in update.parents


class TestGenerateMounts:

    def test(self, plugin, secret, service):
        plugin._swarm_ueid = cobe.UEID('86063fb5b7e8a5ae9ef249972f656b18')
        plugin._secrets = {secret.id: secret}
        plugin._services = [(service, service.tasks())]  # Has two secrets
        updates = list(plugin._generate_mounts())
        assert len(updates) == 2
        update_1, update_2 = updates
        # First secret
        assert update_1.metype == 'Docker:Mount'
        assert update_1.label == 'cobe-agent-test-key'
        assert update_1.attrs.get('container').value == (
            'adab79cca4e176592db386a10d8b964'
            'fecd4c8a3c4b5707e6b217598adeef802'
        )
        assert update_1.attrs.get('container').traits == {'entity:id'}
        assert update_1.attrs.get('target').value \
            == '/var/secrets/cobe-agent-test-key'
        assert update_1.attrs.get('target').traits == {'entity:id'}
        assert update_1.attrs.get('secret:permissions').value == '?r--r--r--'
        assert update_1.attrs.get('secret:permissions').traits == set()
        assert update_1.attrs.get('secret:gid').value == '0'
        assert update_1.attrs.get('secret:uid').value == '0'
        assert len(update_1.parents) == 3
        assert len(update_1.children) == 0
        assert plugin.get_ueid('yr3bkkujr0yv4feillj0v2kbn') in update_1.parents
        assert entityd.docker.get_ueid(
            'DockerService', 'ocipr9lmcjdebr6jo64e8nqtf') in update_1.parents
        assert entityd.docker.get_ueid(
            'DockerContainer',
            'adab79cca4e176592db386a10d8b964fecd4c8a3c4b5707e6b217598adeef802',
        ) in update_1.parents
        # Second secret
        assert update_2.metype == 'Docker:Mount'
        assert update_2.label == 'cobe-agent-test-key-receiver'
        assert update_2.attrs.get('container').value == (
            'adab79cca4e176592db386a10d8b964'
            'fecd4c8a3c4b5707e6b217598adeef802'
        )
        assert update_2.attrs.get('container').traits == {'entity:id'}
        assert update_2.attrs.get('target').value \
            == '/var/secrets/cobe-agent-test-key-receiver'
        assert update_2.attrs.get('target').traits == {'entity:id'}
        assert update_2.attrs.get('secret:permissions').value == '?r--r--r--'
        assert update_2.attrs.get('secret:permissions').traits == set()
        assert update_2.attrs.get('secret:gid').value == '0'
        assert update_2.attrs.get('secret:uid').value == '0'
        assert len(update_2.parents) == 3
        assert len(update_2.children) == 0
        assert plugin.get_ueid('5hvkpeo6z2ez17c6r6g9caxy8') in update_2.parents
        assert entityd.docker.get_ueid(
            'DockerService', 'ocipr9lmcjdebr6jo64e8nqtf') in update_2.parents
        assert entityd.docker.get_ueid(
            'DockerContainer',
            'adab79cca4e176592db386a10d8b964fecd4c8a3c4b5707e6b217598adeef802',
        ) in update_2.parents


class TestGetUEID:

    def test_instance(self, plugin, secret):
        assert str(plugin.get_ueid(secret.id)) \
            == 'a03dcba5b08c46fde555b3ef99a9202b'

    def test_class(self, plugin, secret):
        assert str(plugin.__class__.get_ueid(secret.id)) \
            == 'a03dcba5b08c46fde555b3ef99a9202b'

    def test_helper(self, secret):
        assert str(entityd.docker.get_ueid('DockerSecret', secret.id)) \
            == 'a03dcba5b08c46fde555b3ef99a9202b'


class TestConfigure:

    def test(self, config, plugin):
        plugin.entityd_configure(config)
        assert set(config.entities.keys()) == {
            'Docker:Secret',
            'Docker:Mount',
        }
        assert len(config.entities['Docker:Secret']) == 1
        assert len(config.entities['Docker:Mount']) == 1
        assert list(config.entities['Docker:Secret'])[0].name \
            == 'entityd.docker.swarm.DockerSecret'
        assert list(config.entities['Docker:Mount'])[0].name \
            == 'entityd.docker.swarm.DockerSecret'


class TestCollectionBefore:

    @pytest.fixture
    def client(self, monkeypatch, client_info, secrets, service):
        """Replace Docker client with a mock.

        The client is configured present it self as a swarm, list
        two fake Docker secrets and a single service.
        """
        monkeypatch.setattr(
            entityd.docker.client.DockerClient,
            'get_client',
            unittest.mock.Mock(),
        )
        client = entityd.docker.client.DockerClient.get_client.return_value
        client.secrets.list.return_value = secrets
        client.services.list.return_value = [service]
        client.info.return_value = client_info
        return client

    def test(self, session, plugin, client_info, secrets, service, client):
        plugin._secrets = {}
        plugin.entityd_collection_before(session)
        assert plugin._swarm_ueid == entityd.docker.swarm.DockerSwarm.get_ueid(
            client_info['Swarm']['Cluster']['ID'])
        assert len(plugin._secrets) == 2
        assert plugin._secrets[secrets[0].id] is secrets[0]
        assert plugin._secrets[secrets[1].id] is secrets[1]
        assert len(plugin._services) == 1
        assert len(plugin._services[0]) == 2
        assert plugin._services[0][0] is service
        assert plugin._services[0][1] == service.tasks()

    def test_unavailable(self, monkeypatch, session, plugin):
        monkeypatch.setattr(
            entityd.docker.client.DockerClient,
            'client_available',
            unittest.mock.Mock(return_value=False),
        )
        assert plugin._secrets == {}
        plugin.entityd_collection_before(session)
        assert plugin._secrets == {}


class TestCollectionAfter:

    def test(self, session, plugin, secret, service):
        plugin._swarm_ueid = '86063fb5b7e8a5ae9ef249972f656b18'
        plugin._secrets = {secret.id: secret}
        plugin._services = [(service, service.tasks())]
        plugin.entityd_collection_after(session, ())
        assert plugin._swarm_ueid is None
        assert plugin._secrets == {}
        assert plugin._services == []


class TestFindEntity:

    @pytest.mark.parametrize('type_', ['Docker:Secret', 'Docker:Mount'])
    def test(self, monkeypatch, plugin, type_):
        plugin._swarm_ueid = cobe.UEID('86063fb5b7e8a5ae9ef249972f656b18')
        generator_function = unittest.mock.Mock()
        monkeypatch.setattr(plugin, '_generate_secrets', generator_function)
        monkeypatch.setattr(plugin, '_generate_mounts', generator_function)
        generator = plugin.entityd_find_entity(type_)
        assert generator is generator_function.return_value
        assert generator_function.call_count == 1
        assert generator_function.call_args[0] == ()
        assert generator_function.call_args[1] == {}

    @pytest.mark.parametrize('type_', ['Docker:Secret', 'Docker:Mount'])
    def test_filtering(self, plugin, type_):
        plugin._swarm_ueid = cobe.UEID('86063fb5b7e8a5ae9ef249972f656b18')
        with pytest.raises(LookupError) as exception:
            plugin.entityd_find_entity(type_, attrs={'foo': 'bar'})
        assert 'filtering not supported' in str(exception.value)

    @pytest.mark.parametrize('type_', ['Docker:Secret', 'Docker:Mount'])
    def test_no_swarm(self, monkeypatch, plugin, type_):
        assert plugin.entityd_find_entity(type_) is None

    def test_type_not_implemented(self, plugin):
        plugin._swarm_ueid = cobe.UEID('86063fb5b7e8a5ae9ef249972f656b18')
        assert plugin.entityd_find_entity('Foo') is None
