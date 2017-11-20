import cobe
import collections        # pylint: disable=too-many-lines
import datetime
import socket
import types

import cobe
import kube
import pytest
import requests

import entityd.entityupdate
from entityd.kubernetes import kubernetes


@pytest.fixture
def cluster(monkeypatch):
    """Replace :class:`kube.Cluster` with a mock.

    :returns: the would-be instance of :class:`kube.Cluster`.
    """
    class_ = pytest.MagicMock(name='Cluster')
    instance = class_.return_value
    instance.__enter__.return_value = instance
    monkeypatch.setattr(kube, 'Cluster', class_)
    return instance


@pytest.fixture
def meta_update(monkeypatch):
    monkeypatch.setattr(
        kubernetes, 'apply_meta_update', pytest.Mock())
    return kubernetes.apply_meta_update


def test_entityd_configure(pm, config):
    plugin = pm.register(kubernetes)
    kubernetes.entityd_configure(config)
    assert set(config.entities.keys()) == {
        'Kubernetes:Container',
        'Kubernetes:Pod',
        'Kubernetes:Namespace',
        'Kubernetes:Pod:Probe',
    }
    for entity_plugins in config.entities.values():
        assert plugin in entity_plugins


def test_sessionstart():
    entity = entityd.entityupdate.EntityUpdate('Foo', ueid='a' * 32)
    def entityd_find_entity(name, attrs):    # pylint: disable=unused-argument
        return [[entity]]
    hooks = types.SimpleNamespace(entityd_find_entity=entityd_find_entity)
    pluginmanager = types.SimpleNamespace(hooks=hooks)
    session = types.SimpleNamespace(pluginmanager=pluginmanager)
    kubernetes.entityd_sessionstart(session)
    assert str(kubernetes._CLUSTER_UEID) == 'a' * 32


def test_sessionstart_no_cluster_ueid():
    def entityd_find_entity(name, attrs):    # pylint: disable=unused-argument
        return []
    hooks = types.SimpleNamespace(entityd_find_entity=entityd_find_entity)
    pluginmanager = types.SimpleNamespace(hooks=hooks)
    session = types.SimpleNamespace(pluginmanager=pluginmanager)
    with pytest.raises(LookupError):
        kubernetes.entityd_sessionstart(session)


class TestFindEntity:

    @pytest.fixture
    def generate_updates(self, monkeypatch):
        monkeypatch.setattr(kubernetes,
                            'generate_updates', pytest.Mock())
        return kubernetes.generate_updates

    @pytest.mark.parametrize(
        ('type_', 'generator_function'),
        kubernetes.ENTITIES_PROVIDED.items(),
    )
    def test(self, generate_updates, type_, generator_function):
        generator = kubernetes.entityd_find_entity(type_)
        assert generator is generate_updates.return_value
        assert generate_updates.call_args[0] == (
            getattr(kubernetes, generator_function), None)

    def test_not_provided(self):
        assert kubernetes.entityd_find_entity('Foo') is None

    @pytest.mark.parametrize(
        'type_',
        kubernetes.ENTITIES_PROVIDED.keys(),
    )
    def test_attrs(self, type_):
        with pytest.raises(LookupError):
            kubernetes.entityd_find_entity(
                type_, {'kubernetes:meta:name': 'foo-entity-bar'})


@pytest.yield_fixture
def unreachable_cluster(monkeypatch):
    socket_ = socket.socket()
    socket_.bind(('127.0.0.1', 0))
    url = 'http://{0}:{1}/'.format(*socket_.getsockname())

    class UnreachableCluster(kube.Cluster):

        def __init__(self):
            super().__init__(url)

    with pytest.raises(requests.ConnectionError):
        with UnreachableCluster() as unreachable:
            unreachable.proxy.get()
    monkeypatch.setattr(kube, 'Cluster', UnreachableCluster)
    yield
    socket_.close()


@pytest.mark.parametrize(
    'update_generator', kubernetes.ENTITIES_PROVIDED.values())
def test_cluster_unreachable(unreachable_cluster, update_generator, session):  # pylint: disable=unused-argument
    generator = kubernetes.generate_updates(
        getattr(kubernetes, update_generator), session)
    assert list(generator) == []


@pytest.mark.parametrize(
    'update_generator', kubernetes.ENTITIES_PROVIDED.values())
def test_uncaught_status_error(update_generator, session):

    def generator(_, session):
        update = yield
        update.label = 'first'
        update = yield
        update.label = 'second'
        raise kube.StatusError
        update = yield  # pylint: disable=unreachable
        update.label = 'third'

    generator.__name__ = update_generator
    generator = kubernetes.generate_updates(generator, session)
    updates = list(generator)
    assert len(updates) == 1
    assert updates[0].label == 'first'


@pytest.mark.usefixtures("cluster_ueid")
class TestApplyMetaUpdate:

    def test(self, session):
        meta = kube.ObjectMeta(pytest.Mock(raw={
            'metadata': {
                'name': 'star',
                'namespace': 'andromeda',
                'resourceVersion': '1234',
                'creationTimestamp': '2015-01-14T17:01:37Z',
                'selfLink': '/api/v1/namespaces/andromeda/pods/star',
                'uid': '7955593e-bae0-11e5-b0b9-42010af00091',
                'labels': {
                    'foo': 'bar',
                    'cobe.io/test': 'yes',
                },
            },
        }))
        update = entityd.entityupdate.EntityUpdate('Foo')
        kubernetes.apply_meta_update(meta, update, session)
        assert update.attrs.get('kubernetes:meta:name').value == 'star'
        assert update.attrs.get(
            'kubernetes:meta:name').traits == {'entity:id'}
        assert update.attrs.get(
            'kubernetes:meta:namespace').value == 'andromeda'
        assert update.attrs.get(
            'kubernetes:meta:namespace').traits == {'entity:id'}
        assert update.attrs.get('kubernetes:meta:version').value == '1234'
        assert update.attrs.get('kubernetes:meta:version').traits == set()
        assert update.attrs.get(
            'kubernetes:meta:created').value == '2015-01-14T17:01:37Z'
        assert update.attrs.get(
            'kubernetes:meta:created').traits == {'chrono:rfc3339'}
        assert update.attrs.get('kubernetes:meta:link').value == (
            '/api/v1/namespaces/andromeda/pods/star')
        assert update.attrs.get('kubernetes:meta:link').traits == {'uri'}
        assert update.attrs.get('kubernetes:meta:uid').value == (
            '7955593e-bae0-11e5-b0b9-42010af00091')
        assert update.attrs.get('kubernetes:meta:uid').traits == set()
        assert update.attrs.get('kubernetes:meta:labels').value == {
            'foo': 'bar',
            'cobe.io/test': 'yes',
        }
        assert update.attrs.get('kubernetes:meta:labels').traits == set()

    def test_missing_namespace(self, session):
        meta = kube.ObjectMeta(pytest.Mock(raw={
            'metadata': {
                'name': 'star',
                'resourceVersion': '1234',
                'creationTimestamp': '2015-01-14T17:01:37Z',
                'selfLink': '/api/v1/namespaces/andromeda/pods/star',
                'uid': '7955593e-bae0-11e5-b0b9-42010af00091',
                'labels': {},
            },
        }))
        update = entityd.entityupdate.EntityUpdate('Foo')
        kubernetes.apply_meta_update(meta, update, session)
        assert {attribute.name for attribute in update.attrs} == {
            'kubernetes:meta:name',
            'kubernetes:meta:version',
            'kubernetes:meta:created',
            'kubernetes:meta:link',
            'kubernetes:meta:uid',
            'kubernetes:meta:labels',
            'cluster'
        }

    def test_missing_labels(self, session):
        meta = kube.ObjectMeta(pytest.Mock(raw={
            'metadata': {
                'name': 'star',
                'namespace': 'andromeda',
                'resourceVersion': '1234',
                'creationTimestamp': '2015-01-14T17:01:37Z',
                'selfLink': '/api/v1/namespaces/andromeda/pods/star',
                'uid': '7955593e-bae0-11e5-b0b9-42010af00091',
            },
        }))
        update = entityd.entityupdate.EntityUpdate('Foo')
        kubernetes.apply_meta_update(meta, update, session)
        assert {attribute.name for attribute in update.attrs} == {
            'kubernetes:meta:name',
            'kubernetes:meta:namespace',
            'kubernetes:meta:version',
            'kubernetes:meta:created',
            'kubernetes:meta:link',
            'kubernetes:meta:uid',
            'kubernetes:meta:labels',
            'cluster'
        }
        assert update.attrs.get('kubernetes:meta:labels').value == {}
        assert update.attrs.get('kubernetes:meta:labels').traits == set()


class TestNamespaces:

    def test(self, cluster, meta_update):
        namespace_resources = [
            kube.NamespaceItem(cluster, {
                'metadata': {
                    'name': 'namespace-1',
                    'namespace': 'andromeda',
                },
                'status': {
                    'phase': 'Active',
                },
            }),
            kube.NamespaceItem(cluster, {
                'metadata': {
                    'name': 'namespace-2',
                },
                'status': {
                    'phase': 'Terminating',
                },
            }),
        ]
        kubernetes._CLUSTER_UEID = cobe.UEID('abcd' * 8)
        cluster.namespaces.__iter__.return_value = iter(namespace_resources)
        namespaces = list(
            kubernetes.entityd_find_entity('Kubernetes:Namespace'))
        assert len(namespaces) == 2
        assert namespaces[0].metype == 'Kubernetes:Namespace'
        assert namespaces[0].label == 'namespace-1'
        assert namespaces[0].attrs.get('phase').value == 'Active'
        assert namespaces[0].attrs.get(
            'phase').traits == {'kubernetes:namespace-phase'}
        assert namespaces[1].metype == 'Kubernetes:Namespace'
        assert namespaces[1].label == 'namespace-2'
        assert namespaces[1].attrs.get('phase').value == 'Terminating'
        assert namespaces[1].attrs.get(
            'phase').traits == {'kubernetes:namespace-phase'}
        assert meta_update.call_count == 2
        assert meta_update.call_args_list[0][0] == (
            namespace_resources[0].meta, namespaces[0], None)
        assert meta_update.call_args_list[1][0] == (
            namespace_resources[1].meta, namespaces[1], None)

@pytest.mark.usefixtures("cluster_ueid")
class TestPods:

    @pytest.fixture
    def namespaces(self, monkeypatch, cluster, meta_update):  # pylint: disable=unused-argument
        updates = []

        def generate_namespaces(cluster):  # pylint: disable=unused-argument
            update = yield
            update.attrs.set('kubernetes:meta:name', 'andromeda')
            updates.append(update)

        monkeypatch.setattr(
            kubernetes, 'generate_namespaces', generate_namespaces)
        return updates

    def test(self, cluster, meta_update):
        pod_resources = [
            kube.PodItem(cluster, {
                'metadata': {
                    'name': 'pod-1',
                    'namespace': 'andromeda',
                },
                'status': {
                    'phase': 'Running',
                    'podIP': '10.120.0.5',
                    'startTime': '2015-01-14T17:01:37Z',
                },
            }),
            kube.PodItem(cluster, {
                'metadata': {
                    'name': 'pod-2',
                    'namespace': 'andromeda',
                },
                'status': {
                    'phase': 'Running',
                    'podIP': '10.120.0.7',
                    'startTime': '2016-01-14T17:01:37Z',
                },
            }),
        ]
        cluster.pods.__iter__.return_value = iter(pod_resources)
        pods = list(kubernetes.entityd_find_entity('Kubernetes:Pod'))
        assert len(pods) == 2
        assert pods[0].metype == 'Kubernetes:Pod'
        assert pods[0].label == 'pod-1'
        assert pods[0].attrs.get('kubernetes:kind').value == 'Pod'
        assert pods[0].attrs.get('kubernetes:kind').traits == set()
        assert pods[0].attrs.get('phase').value == 'Running'
        assert pods[0].attrs.get('phase').traits == {'kubernetes:pod-phase'}
        assert pods[0].attrs.get('start_time').value == '2015-01-14T17:01:37Z'
        assert pods[0].attrs.get('start_time').traits == {'chrono:rfc3339'}
        assert pods[0].attrs.get('ip').value == '10.120.0.5'
        assert pods[0].attrs.get('ip').traits == {'ipaddr:v4'}
        assert list(pods[0].parents) == []
        assert pods[1].metype == 'Kubernetes:Pod'
        assert pods[1].label == 'pod-2'
        assert pods[1].attrs.get('phase').value == 'Running'
        assert pods[1].attrs.get('phase').traits == {'kubernetes:pod-phase'}
        assert pods[1].attrs.get('start_time').value == '2016-01-14T17:01:37Z'
        assert pods[1].attrs.get('start_time').traits == {'chrono:rfc3339'}
        assert pods[1].attrs.get('ip').value == '10.120.0.7'
        assert pods[1].attrs.get('ip').traits == {'ipaddr:v4'}
        assert meta_update.call_count == 2
        assert meta_update.call_args_list[0][0] == (
            pod_resources[0].meta, pods[0], None)
        assert meta_update.call_args_list[1][0] == (
            pod_resources[1].meta, pods[1], None)

    def test_no_ip_attribute(self, cluster, session):
        pod = kube.PodItem(
            cluster, {
                'metadata': {
                    'name': 'pod-1',
                    'namespace': 'andromeda',
                    'resourceVersion': '1234',
                    'creationTimestamp': '2015-01-14T17:01:37Z',
                    'selfLink': '/api/v1/namespaces/andromeda/pods/star',
                    'uid': '7955593e-bae0-11e5-b0b9-42010af00091',
                },
                'status': {
                    'phase': 'Running',
                    'startTime': '2016-01-14T17:01:37Z',
                    'message': 'Once upon a time ...',
                },
            })
        update = entityd.entityupdate.EntityUpdate('Foo', ueid='a' * 32)
        update.attrs.set('ip', 'test')
        assert 'ip' in update.attrs._attrs
        update = kubernetes.pod_update(pod, update, session)
        assert 'ip' not in update.attrs._attrs

    def test_with_message(self, cluster, meta_update):
        pod_resources = [
            kube.PodItem(cluster, {
                'metadata': {
                    'name': 'pod-1',
                    'namespace': 'andromeda',
                },
                'status': {
                    'phase': 'Running',
                    'podIP': '10.120.0.7',
                    'startTime': '2016-01-14T17:01:37Z',
                    'message': 'Once upon a time ...',
                },
            }),
        ]
        cluster.pods.__iter__.return_value = iter(pod_resources)
        pods = list(kubernetes.entityd_find_entity('Kubernetes:Pod'))
        assert len(pods) == 1
        assert pods[0].metype == 'Kubernetes:Pod'
        assert pods[0].label == 'pod-1'
        assert pods[0].attrs.get('message').value == 'Once upon a time ...'
        assert pods[0].attrs.get('message').traits == set()
        assert meta_update.call_count == 1
        assert meta_update.call_args_list[0][0] == (
            pod_resources[0].meta, pods[0], None)

    def test_with_reason(self, cluster, meta_update):
        pod_resources = [
            kube.PodItem(cluster, {
                'metadata': {
                    'name': 'pod-1',
                    'namespace': 'andromeda',
                },
                'status': {
                    'phase': 'Running',
                    'podIP': '10.120.0.7',
                    'startTime': '2016-01-14T17:01:37Z',
                    'reason': 'ItsWorking',
                },
            }),
        ]
        cluster.pods.__iter__.return_value = iter(pod_resources)
        pods = list(kubernetes.entityd_find_entity('Kubernetes:Pod'))
        assert len(pods) == 1
        assert pods[0].metype == 'Kubernetes:Pod'
        assert pods[0].label == 'pod-1'
        assert pods[0].attrs.get('reason').value == 'ItsWorking'
        assert pods[0].attrs.get('reason').traits == set()
        assert meta_update.call_count == 1
        assert meta_update.call_args_list[0][0] == (
            pod_resources[0].meta, pods[0], None)

    def test_ipv6(self, cluster, meta_update):
        pod_resources = [
            kube.PodItem(cluster, {
                'metadata': {
                    'name': 'pod-1',
                    'namespace': 'andromeda',
                },
                'status': {
                    'phase': 'Running',
                    'podIP': '2001:db8::8:800:200c:417a',
                    'startTime': '2016-01-14T17:01:37Z',
                },
            }),
        ]
        cluster.pods.__iter__.return_value = iter(pod_resources)
        pods = list(kubernetes.entityd_find_entity('Kubernetes:Pod'))
        assert len(pods) == 1
        assert pods[0].metype == 'Kubernetes:Pod'
        assert pods[0].label == 'pod-1'
        assert pods[0].attrs.get('ip').value == '2001:db8::8:800:200c:417a'
        assert pods[0].attrs.get('ip').traits == {'ipaddr:v6'}
        assert meta_update.call_count == 1
        assert meta_update.call_args_list[0][0] == (
            pod_resources[0].meta, pods[0], None)


@pytest.mark.usefixtures("cluster_ueid")
class TestProbes:

    @pytest.fixture
    def raw_pods_resource(self):
        return  [{
                'metadata': {
                    'name': 'pod-1',
                    'namespace': 'andromeda',
                    'resourceVersion': '12345678',
                    'creationTimestamp': '2017-09-01T14:11:03Z',
                    'selfLink': '',
                    'uid': 'aaaabbbbccccddddeeeeffffgggghhhhiiii',
                },
                'spec': {
                    'containers': [
                        {
                            'livenessProbe': {
                                'failureThreshold': 3,
                                'initialDelaySeconds': 15,
                                'periodSeconds': 20,
                                'successThreshold': 1,
                                'tcpSocket': {
                                    'port': 8080
                                },
                                'timeoutSeconds': 1
                            },
                         }
                    ]
                },
                'status': {
                    'phase': 'Running',
                    'podIP': '10.120.0.5',
                    'startTime': '2015-01-14T17:01:37Z',
                },
            },

             {
                'metadata': {
                    'name': 'pod-2',
                    'namespace': 'andromeda',
                    'resourceVersion': '12345678',
                    'creationTimestamp': '2017-09-01T14:11:03Z',
                    'selfLink': '',
                    'uid': 'aaaabbbbccccddddeeeeffffgggghhhhiiii',
                },
                'spec': {
                    'containers': [
                        {
                            'readinessProbe': {
                                'failureThreshold': 3,
                                'initialDelaySeconds': 15,
                                'periodSeconds': 20,
                                'successThreshold': 1,
                                'httpGet': {
                                    'path': '/#/status',
                                    'port': 9093,
                                    'scheme': 'HTTP'
                                },
                                'timeoutSeconds': 1
                            },
                        }
                    ]
                },
                'status': {
                    'phase': 'Running',
                    'podIP': '10.120.0.7',
                    'startTime': '2016-01-14T17:01:37Z',
                },
            },

             {
                'metadata': {
                    'name': 'pod-3',
                    'namespace': 'andromeda',
                    'resourceVersion': '12345678',
                    'creationTimestamp': '2017-09-01T14:11:03Z',
                    'selfLink': '',
                    'uid': 'aaaabbbbccccddddeeeeffffgggghhhhiiii',

                },
                'spec': {
                    'containers': [
                        {
                            'livenessProbe': {
                                'exec': {
                                    'command': [
                                        'entityd-health-check'
                                    ]
                                },
                                'failureThreshold': 3,
                                'initialDelaySeconds': 15,
                                'periodSeconds': 20,
                                'successThreshold': 1,
                                'timeoutSeconds': 1
                            },

                        }
                    ]
                },
                'status': {
                    'phase': 'Running',
                    'podIP': '10.120.0.7',
                    'startTime': '2016-01-14T17:01:37Z',
                },
            },
        ]

    def test(self, cluster, raw_pods_resource):
        for i,pod_resource in enumerate(raw_pods_resource):
            pod = kube.PodItem(cluster, pod_resource)
            cluster.pods.__iter__.return_value = iter([pod])
            cluster.pods.fetch.return_value = pod
            probes = \
                list(kubernetes.entityd_find_entity('Kubernetes:Pod:Probe'))
            assert len(probes) == 1
            probe = probes[0]
            assert probe.attrs.get('failure-threshold').value == 3
            assert probe.attrs.get('failure-threshold').traits == set()
            assert probe.attrs.get('initial-delay-seconds').value == 15
            assert probe.attrs.get('initial-delay-seconds').traits == set()
            assert probe.attrs.get('period-seconds').value == 20
            assert probe.attrs.get('period-seconds').traits == set()
            assert probe.attrs.get('success-threshold').value == 1
            assert probe.attrs.get('success-threshold').traits == set()
            assert probe.attrs.get('timeout-seconds').value == 1
            assert probe.attrs.get('timeout-seconds').traits == set()
            assert probe.attrs.get('kubernetes:pod').traits == {'entity:id'}
            assert probe.attrs.get('kubernetes:probe:type').traits ==\
                   {'entity:id'}
            if i == 0:
                assert probe.attrs.get('kubernetes:probe:type').value ==\
                       "Liveness probe"
                assert probe.attrs.get('tcpSocket:port').value == 8080
                assert probe.attrs.get('tcpSocket:port').traits == set()
            elif i == 1:
                assert probe.attrs.get('kubernetes:probe:type').value ==\
                       "Readiness probe"
                assert probe.attrs.get('httpGet:path').value == '/#/status'
                assert probe.attrs.get('httpGet:path').traits == set()
                assert probe.attrs.get('httpGet:port').value == 9093
                assert probe.attrs.get('httpGet:port').traits == set()
                assert probe.attrs.get('httpGet:scheme').value == 'HTTP'
                assert probe.attrs.get('httpGet:scheme').traits == set()
            else:
                assert probe.attrs.get('kubernetes:probe:type').value ==\
                       "Liveness probe"
                assert len(probe.attrs.get('exec:command').value) == 1
                assert probe.attrs.get('exec:command').value[0] == \
                       'entityd-health-check'
                assert probe.attrs.get('exec:command').traits == set()

    @pytest.mark.parametrize("miss_attr", [
        'failureThreshold',
        'initialDelaySeconds',
        'periodSeconds',
        'successThreshold',
        'timeoutSeconds',
    ])
    def test_missing_attribute(self, cluster, raw_pods_resource, miss_attr):
        pod_resource = raw_pods_resource[0]
        del(pod_resource['spec']['containers'][0]['livenessProbe'][miss_attr])
        print(str(pod_resource['spec']['containers'][0]))
        pod = kube.PodItem(cluster, pod_resource)
        cluster.pods.__iter__.return_value = iter([pod])
        cluster.pods.fetch.return_value = pod
        assert list(kubernetes.entityd_find_entity('Kubernetes:Pod:Probe'))

    def test_missing_namespace(self, cluster, raw_pods_resource):
        pod = kube.PodItem(cluster, raw_pods_resource[0])
        cluster.pods.__iter__.return_value = iter([pod])
        cluster.namespaces.fetch.side_effect = LookupError
        probes = list(kubernetes.entityd_find_entity('Kubernetes:Pod:Probe'))
        assert not probes

    def test_missing_pod(self, cluster, raw_pods_resource):
        pod = kube.PodItem(cluster, raw_pods_resource[0])
        cluster.pods.__iter__.return_value = iter([pod])
        cluster.pods.fetch.side_effect = LookupError
        mock_namespace = cluster.namespaces.fetch.return_value
        mock_namespace.pods.fetch.side_effect = LookupError
        probes = list(kubernetes.entityd_find_entity('Kubernetes:Pod:Probe'))
        assert not probes


@pytest.mark.usefixtures("cluster_ueid")
class TestContainers:

    @pytest.fixture
    def raw_pod_resource(self):
        """Note that for testing 2 of the pod's containers have no containerID.

        Containers with no containerID attribute represent containers that are
        in a state of `ContainerCreating`. These containers enable
        testing to show that entities are not created for such containers.
        """
        return {
            'metadata': {
                'name': 'pod',
                'namespace': 'andromeda',
                'resourceVersion': '1234',
                'creationTimestamp': '2015-01-14T17:01:37Z',
                'selfLink': '/api/v1/namespaces/andromeda/pods/star',
                'uid': '7955593e-bae0-11e5-b0b9-42010af00091',
            },
            'status': {
                'phase': 'Running',
                'podIP': '10.120.0.5',
                'startTime': '2015-01-14T17:01:37Z',
                'containerStatuses': [
                    {
                        'name': 'container-1',
                        'containerID': (
                            'docker://3a542701e9896f6a4e526cc69e'
                            '6191b221cf29e1cabb43edf3b47fe5b33a7a59'
                        ),
                        'imageID': (
                            'docker://33688d2af35f810373734d5928'
                            'f3e7c579e2569aa80ed80580436f1fd90e53c6'
                        ),
                        'image': 'repository/user/image:tag',
                        'ready': True,
                        'state': {
                            'running': {
                                'startedAt': '2015-12-04T19:15:23Z',
                            }
                        },
                        'restartCount': 0,
                    },
                    {
                        'name': 'container_with_no_containerID attribute',
                        'imageID': (
                            'docker://33688d2af35f810373734d5928'
                            'f3e7c579e2569aa80ed80580436f1fd90e53c6'
                        ),
                        'image': 'repository/user/image:tag',
                        'ready': True,
                        'state': {
                            'running': {
                                'startedAt': '2015-12-04T19:15:23Z',
                            }
                        },
                        'restartCount': 0,
                    },
                    {
                        'name': 'container_with_no_containerID attribute',
                        'imageID': (
                            'docker://33688d2af35f810373734d5928'
                            'f3e7c579e2569aa80ed80580436f1fd90e53c6'
                        ),
                        'image': 'repository/user/image:tag',
                        'ready': True,
                        'state': {
                            'running': {
                                'startedAt': '2015-12-04T19:15:23Z',
                            }
                        },
                        'restartCount': 0,
                    },
                ],
            },
            'spec': {
                'containers': [
                    {
                        'name': 'container-1',
                        'resources': {},
                    },
                    {
                        'name': 'container_with_no_containerID attribute',
                        'resources': {},
                    },
                    {
                        'name': 'container_with_no_containerID attribute',
                        'resources': {},
                    }
                ],
            },
        }

    @pytest.mark.parametrize(
        ('resources', 'lim_mem', 'lim_cpu', 'req_mem', 'req_cpu'), [
            ({
                'limits': {
                    'memory': '100Mi',
                    'cpu': '700m',
                },
                'requests': {
                    'memory': '50Mi',
                    'cpu': '600m',
                },
            }, 104857600, 70, 52428800, 60),
        ])
    def test(self, cluster, raw_pod_resource,
             resources, lim_mem, lim_cpu, req_mem, req_cpu, loghandler):
        raw_pod_resource['spec']['containers'].insert(
            0, {'name': 'container-1', 'resources': resources})
        pod = kube.PodItem(cluster, raw_pod_resource)
        cluster.pods.__iter__.return_value = iter([pod])
        cluster.pods.fetch.return_value = pod
        containers = list(kubernetes.entityd_find_entity('Kubernetes:Container'))
        assert len(containers) == 1
        assert containers[0].metype == 'Kubernetes:Container'
        assert containers[0].label == 'container-1'
        assert containers[0].attrs.get('id').value == (
            'docker://3a542701e9896f6a4e526cc69e6'
            '191b221cf29e1cabb43edf3b47fe5b33a7a59')
        assert containers[0].attrs.get('id').traits == {'entity:id'}
        assert containers[0].attrs.get('name').value == 'container-1'
        assert containers[0].attrs.get('name').traits == set()
        assert containers[0].attrs.get('kubernetes:kind').value == 'Container'
        assert containers[0].attrs.get('kubernetes:kind').traits == set()
        assert containers[0].attrs.get('manager').value == 'Docker'
        assert containers[0].attrs.get('manager').traits == set()
        assert containers[0].attrs.get('ready').value is True
        assert containers[0].attrs.get('ready').traits == set()
        assert containers[0].attrs.get('image:id').value == (
            'docker://33688d2af35f810373734d5928f'
            '3e7c579e2569aa80ed80580436f1fd90e53c6')
        assert containers[0].attrs.get('image:id').traits == set()
        assert containers[0].attrs.get('image:name').value == (
            'repository/user/image:tag')
        assert containers[0].attrs.get('image:name').traits == set()
        assert containers[0].attrs.get(
            'resources:requests:memory').value == req_mem
        assert containers[0].attrs.get(
            'resources:requests:memory').traits == {'unit:bytes'}
        assert containers[0].attrs.get(
            'resources:requests:cpu').value == req_cpu
        assert containers[0].attrs.get(
            'resources:requests:cpu').traits == {'unit:percent'}
        assert containers[0].attrs.get(
            'resources:limits:memory').value == lim_mem
        assert containers[0].attrs.get(
            'resources:limits:memory').traits == {'unit:bytes'}
        assert containers[0].attrs.get(
            'resources:limits:cpu').value == lim_cpu
        assert containers[0].attrs.get(
            'resources:limits:cpu').traits == {'unit:percent'}
        assert loghandler.has_error() is False

    def test_resources_errors(self, cluster, raw_pod_resource, loghandler):
        resources = {
            'limits': {
                'memory': 'something unexpected from k8s',
                'cpu': 'something unexpected from k8s',
            },
            'requests': {
                'memory': 'something unexpected from k8s',
                'cpu': 'something unexpected from k8s',
            },
        }
        raw_pod_resource['spec']['containers'].insert(
            0, {'name': 'container-1', 'resources': resources})
        pod = kube.PodItem(cluster, raw_pod_resource)
        cluster.pods.__iter__.return_value = iter([pod])
        cluster.pods.fetch.return_value = pod
        containers = list(kubernetes.entityd_find_entity('Kubernetes:Container'))
        for attribute in ['resources:requests:memory',
                          'resources:requests:cpu',
                          'resources:limits:memory',
                          'resources:limits:cpu']:
            with pytest.raises(KeyError):
                containers[0].attrs.get(attribute)
        assert loghandler.has_error() is True


    def test_running(self, cluster, raw_pod_resource):
        raw_pod_resource['status']['containerStatuses'][0]['state'] = {
            'running': {
                'startedAt': '2015-12-04T19:15:23Z',
            }
        }
        pod = kube.PodItem(cluster, raw_pod_resource)
        cluster.pods.__iter__.return_value = iter([pod])
        cluster.pods.fetch.return_value = pod
        container = list(kubernetes.entityd_find_entity('Kubernetes:Container'))[0]
        assert container.attrs.get(
            'state:started-at').value == '2015-12-04T19:15:23Z'
        assert container.attrs.get(
            'state:started-at').traits == {'chrono:rfc3339'}
        assert container.attrs.deleted() == {
            'state:reason',
            'state:exit-code',
            'state:signal',
            'state:message',
            'state:finished-at',
        }

    def test_waiting(self, cluster, raw_pod_resource):
        raw_pod_resource['status']['containerStatuses'][0]['state'] = {
            'waiting': {
                'reason': 'FooBar',
            }
        }
        pod = kube.PodItem(cluster, raw_pod_resource)
        cluster.pods.__iter__.return_value = iter([pod])
        cluster.pods.fetch.return_value = pod
        container = list(kubernetes.entityd_find_entity('Kubernetes:Container'))[0]
        assert container.attrs.get('state:reason').value == 'FooBar'
        assert container.attrs.get('state:reason').traits == set()
        assert container.attrs.deleted() == {
            'state:started-at',
            'state:exit-code',
            'state:signal',
            'state:message',
            'state:finished-at',
        }

    def test_terminated(self, cluster, raw_pod_resource):
        raw_pod_resource['status']['containerStatuses'][0]['state'] = {
            'terminated': {
                'startedAt': '2015-12-04T19:15:23Z',
                'finishedAt': '2016-12-04T19:15:23Z',
                'reason': 'ItsDeadJim',
                'message': '...',
                'exitCode': 0,
                'signal': 15,
            }
        }
        pod = kube.PodItem(cluster, raw_pod_resource)
        cluster.pods.__iter__.return_value = iter([pod])
        cluster.pods.fetch.return_value = pod
        container = list(kubernetes.entityd_find_entity('Kubernetes:Container'))[0]
        assert container.attrs.get(
            'state:started-at').value == '2015-12-04T19:15:23Z'
        assert container.attrs.get(
            'state:started-at').traits == {'chrono:rfc3339'}
        assert container.attrs.get(
            'state:finished-at').value == '2016-12-04T19:15:23Z'
        assert container.attrs.get(
            'state:finished-at').traits == {'chrono:rfc3339'}
        assert container.attrs.get('state:reason').value == 'ItsDeadJim'
        assert container.attrs.get('state:reason').traits == set()
        assert container.attrs.get('state:exit-code').value == 0
        assert container.attrs.get('state:exit-code').traits == set()
        assert container.attrs.get('state:signal').value == 15
        assert container.attrs.get('state:signal').traits == set()
        assert container.attrs.get('state:message').value == '...'
        assert container.attrs.get('state:message').traits == set()
        assert container.attrs.deleted() == set()

    def test_no_ip_attribute(self, cluster, raw_pod_resource):
        raw_pod_resource['status']['containerStatuses'][0]['state'] = {
            'terminated': {
                'startedAt': '2015-12-04T19:15:23Z',
                'finishedAt': '2016-12-04T19:15:23Z',
                'reason': 'ItsDeadJim',
                'exitCode': 0,
            }
        }
        pod = kube.PodItem(cluster, raw_pod_resource)
        cluster.pods.__iter__.return_value = iter([pod])
        cluster.pods.fetch.return_value = pod
        container = list(kubernetes.entityd_find_entity('Kubernetes:Container'))[0]
        assert 'state:signal' not in container.attrs._attrs
        assert 'state:message' not in container.attrs._attrs

    def test_missing_namespace(self, cluster, raw_pod_resource):
        pod = kube.PodItem(cluster, raw_pod_resource)
        cluster.pods.__iter__.return_value = iter([pod])
        cluster.namespaces.fetch.side_effect = LookupError
        containers = list(kubernetes.entityd_find_entity('Kubernetes:Container'))
        assert not containers

    def test_missing_pod(self, cluster, raw_pod_resource):
        pod = kube.PodItem(cluster, raw_pod_resource)
        cluster.pods.__iter__.return_value = iter([pod])
        mock_namespace = cluster.namespaces.fetch.return_value
        mock_namespace.pods.fetch.side_effect = LookupError
        containers = list(kubernetes.entityd_find_entity('Kubernetes:Container'))
        assert not containers


class TestMetric:

    def test_repr(self):
        metric = kubernetes.Metric('foo', ('A', 'B'), {'spam', 'eggs'})
        assert repr(metric) == '<Metric foo @ A.B traits: eggs, spam>'

    def test(self):
        metric = kubernetes.Metric('foo', ('A', 'B'), {'trait'})
        update = entityd.entityupdate.EntityUpdate('Entity')
        object_ = {'A': {'B': 'value'}}
        metric.apply(object_, update)
        attribute = update.attrs.get('foo')
        assert attribute.name == 'foo'
        assert attribute.value is object_['A']['B']
        assert attribute.traits == {'trait'}

    def test_unresolvable(self):
        metric = kubernetes.Metric('foo', ('A', 'B'), {'trait'})
        update = entityd.entityupdate.EntityUpdate('Entity')
        object_ = {'A': {'C': 'value'}}
        metric.apply(object_, update)
        assert update.attrs.deleted() == {'foo'}
        with pytest.raises(KeyError):
            update.attrs.get('foo')

    def test_with_prefix(self):
        metric = kubernetes.Metric('bar', ('B', 'C'), {'trait'})
        update = entityd.entityupdate.EntityUpdate('Entity')
        object_ = {'A': {'B': {'C': 'value'}}}
        metric.with_prefix('foo', ('A',)).apply(object_, update)
        attribute = update.attrs.get('foo:bar')
        assert attribute.name == 'foo:bar'
        assert attribute.value is object_['A']['B']['C']
        assert attribute.traits == {'trait'}

    def test_nanosecond(self):
        metric = kubernetes.NanosecondMetric('foo', ('A',), set())
        update = entityd.entityupdate.EntityUpdate('Entity')
        object_ = {'A': 123000000000}
        metric.apply(object_, update)
        attribute = update.attrs.get('foo')
        assert attribute.name == 'foo'
        assert attribute.value == 123
        assert attribute.traits == set()

    def test_millisecond(self):
        metric = kubernetes.MillisecondMetric('foo', ('A',), set())
        update = entityd.entityupdate.EntityUpdate('Entity')
        object_ = {'A': 123000000}
        metric.apply(object_, update)
        attribute = update.attrs.get('foo')
        assert attribute.name == 'foo'
        assert attribute.value == 123
        assert attribute.traits == set()


class TestNearestPoint:

    def test(self):
        target = datetime.datetime(2000, 8, 1)
        points = [kubernetes.Point(t, {}) for t in (
            target + datetime.timedelta(seconds=-1),
            target + datetime.timedelta(seconds=0),
            target + datetime.timedelta(seconds=1),
        )]
        point = kubernetes.select_nearest_point(target, points, 5.0)
        assert point is points[1]

    def test_no_points(self):
        target = datetime.datetime(2000, 8, 1)
        with pytest.raises(ValueError):
            kubernetes.select_nearest_point(target, [], 5.0)

    @pytest.mark.parametrize('delta', [-20.1 * 60, 20.1 * 60])
    def test_exceed_threshold(self, delta):
        target = datetime.datetime(2000, 8, 1)
        points = [kubernetes.Point(
            target + datetime.timedelta(seconds=delta), {})]
        with pytest.raises(ValueError):
            kubernetes.select_nearest_point(target,
                                            points, threshold=20 * 60)

    @pytest.mark.parametrize('threshold', [1, 5, 10, 20])
    def test_threshold_used_ok(self, threshold):
        target = datetime.datetime(2000, 8, 1)
        points = [
            kubernetes.Point(
                target + datetime.timedelta(seconds=threshold), {})
        ]
        point = kubernetes.select_nearest_point(target, points,
                                                threshold=threshold)
        assert point == points[0]

    @pytest.mark.parametrize('threshold', [1, 5, 10, 20])
    def test_threshold_used_exceed(self, threshold):
        target = datetime.datetime(2000, 8, 1)
        points = [
            kubernetes.Point(
                target + datetime.timedelta(seconds=threshold + 1), {})
        ]
        with pytest.raises(ValueError):
            kubernetes.select_nearest_point(target, points,
                                            threshold=threshold)


class TestCAdvisorToPoints:

    @pytest.mark.parametrize('fraction', ['0', '000000', '000000000'])
    @pytest.mark.parametrize(('timestamp', 'offset'), [
        ('2000-08-01T12:00:00', 'Z'),
        ('2000-08-01T16:00:00', '+04:00'),
        ('2000-08-01T06:00:00', '-06:00'),
    ])
    def test(self, fraction, timestamp, offset):
        raw_points = [{'timestamp': timestamp + '.' + fraction + offset}]
        points = kubernetes.cadvisor_to_points(raw_points)
        assert len(points) == 1
        assert points[0].timestamp == datetime.datetime(2000, 8, 1, 12)
        assert points[0].data is raw_points[0]

    def test_invalid_offset_separator(self):
        raw_points = [
            {'timestamp': '2000-08-01T10:00:00.0Z'},
            {'timestamp': '2000-08-01T12:00:00.0#00:00'},
            {'timestamp': '2000-08-01T14:00:00.0Z'},
        ]
        points = kubernetes.cadvisor_to_points(raw_points)
        assert len(points) == 2
        assert points[0].timestamp == datetime.datetime(2000, 8, 1, 10)
        assert points[0].data is raw_points[0]
        assert points[1].timestamp == datetime.datetime(2000, 8, 1, 14)
        assert points[1].data is raw_points[2]


class TestSimpleMetrics:

    def test(self, monkeypatch):
        point = kubernetes.Point(pytest.Mock(), {
            'foo': 50,
            'bar': 100,
        })
        monkeypatch.setattr(
            kubernetes,
            'METRICS_CONTAINER',
            [
                kubernetes.Metric('foo', ('foo',), set()),
                kubernetes.Metric('bar', ('bar',), set()),
            ],
        )
        update = entityd.entityupdate.EntityUpdate('Entity')
        kubernetes.simple_metrics(point, update)
        attribute_foo = update.attrs.get('foo')
        attribute_bar = update.attrs.get('bar')
        assert attribute_foo.value == 50
        assert attribute_foo.traits == set()
        assert attribute_bar.value == 100
        assert attribute_bar.traits == set()


class TestFileSystemMetrics:

    def test(self, monkeypatch):
        point = kubernetes.Point(pytest.Mock(), {
            'filesystem': [
                {
                    'device': '/dev/disk/by-uuid/foo',
                    'spam': 50,
                },
                {
                    'device': '/dev/disk/by-uuid/bar',
                    'spam': 100,
                },
            ],
        })
        monkeypatch.setattr(
            kubernetes,
            'METRICS_FILESYSTEM',
            [kubernetes.Metric('attribute', ('spam',), set())],
        )
        update = entityd.entityupdate.EntityUpdate('Entity')
        kubernetes.filesystem_metrics(point, update)
        attribute_foo = update.attrs.get('filesystem:foo:attribute')
        attribute_bar = update.attrs.get('filesystem:bar:attribute')
        assert attribute_foo.value == 50
        assert attribute_foo.traits == set()
        assert attribute_bar.value == 100
        assert attribute_bar.traits == set()

    def test_no_filesystem_field(self):
        point = kubernetes.Point(pytest.Mock(), {})
        update = entityd.entityupdate.EntityUpdate('Entity')
        kubernetes.filesystem_metrics(point, update)
        assert len(list(update.attrs)) == 0


class TestDiskIOMetrics:

    def test(self, monkeypatch):
        point = kubernetes.Point(pytest.Mock(), {
            'diskio': {
                'foo': [
                    {
                        'major': 8,
                        'minor': 0,
                        'stats': {
                            'value': 50,
                        },
                    },
                    {
                        'major': 16,
                        'minor': 0,
                        'stats': {
                            'value': 100,
                        },
                    },
                ],
            },
        })
        monkeypatch.setattr(
            kubernetes,
            'METRICS_DISKIO',
            {'foo': [kubernetes.Metric(
                'attribute', ('stats', 'value',), set())]},
        )
        update = entityd.entityupdate.EntityUpdate('Entity')
        kubernetes.diskio_metrics(point, update)
        attribute_8_0 = update.attrs.get('io:8:0:attribute')
        attribute_16_0 = update.attrs.get('io:16:0:attribute')
        assert attribute_8_0.value == 50
        assert attribute_8_0.traits == set()
        assert attribute_16_0.value == 100
        assert attribute_16_0.traits == set()

    def test_missing_key(self, monkeypatch):
        # Order matters as we want to ensure that it continues even
        # after failing to find the first 'foo' field.
        metrics = collections.OrderedDict([
            ('foo', [kubernetes.Metric(
                'foo', ('stats', 'value'), set())]),
            ('bar', [kubernetes.Metric(
                'bar', ('stats', 'value'), set())]),
        ])
        point = kubernetes.Point(pytest.Mock(), {
            'diskio': {
                'bar': [
                    {
                        'major': 8,
                        'minor': 0,
                        'stats': {
                            'value': 50,
                        },
                    },
                ],
            },
        })
        monkeypatch.setattr(kubernetes, 'METRICS_DISKIO', metrics)
        update = entityd.entityupdate.EntityUpdate('Entity')
        kubernetes.diskio_metrics(point, update)
        with pytest.raises(KeyError):
            update.attrs.get('io:8:0:foo')
        attribute = update.attrs.get('io:8:0:bar')
        assert attribute.value == 50
        assert attribute.traits == set()


class TestContainerMetrics:

    @pytest.fixture()
    def metrics(self, monkeypatch):
        monkeypatch.setattr(kubernetes,
                            'simple_metrics', pytest.Mock())
        monkeypatch.setattr(kubernetes,
                            'filesystem_metrics', pytest.Mock())
        monkeypatch.setattr(kubernetes,
                            'diskio_metrics', pytest.Mock())
        return (kubernetes.simple_metrics,
                kubernetes.filesystem_metrics,
                kubernetes.diskio_metrics)

    @pytest.mark.parametrize('key', ['/foo', '/system.slice/docker-foo'])
    def test(self, cluster, metrics, key):
        point_data = {
            'timestamp':
                datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')}
        pod = pytest.Mock(cluster=cluster)
        container = kube.Container(pod, {'containerID': 'docker://foo'})
        update = entityd.entityupdate.EntityUpdate('Entity')
        cluster.nodes = [
            kube.NodeItem(cluster, {'metadata': {'name': 'node'}})]
        cluster.proxy.get.return_value = {key: [point_data]}
        kubernetes.container_metrics(container, update)
        assert metrics[0].called
        assert metrics[1].called
        assert metrics[2].called
        assert metrics[0].call_args[0][0].data is point_data
        assert metrics[0].call_args[0][1] is update
        assert metrics[1].call_args[0][0].data is point_data
        assert metrics[1].call_args[0][1] is update
        assert metrics[2].call_args[0][0].data is point_data
        assert metrics[2].call_args[0][1] is update

    def test_apierror(self, cluster, metrics):
        pod = pytest.Mock(cluster=cluster)
        container = kube.Container(pod, {'containerID': 'foo'})
        update = entityd.entityupdate.EntityUpdate('Entity')
        cluster.nodes = [
            kube.NodeItem(cluster, {'metadata': {'name': 'node'}})]
        cluster.proxy.get.side_effect = kube.APIError(pytest.Mock())
        kubernetes.container_metrics(container, update)
        assert not metrics[0].called
        assert not metrics[1].called
        assert not metrics[2].called

    def test_timestamp_threshold(self, cluster, metrics):
        point_data = {
            'timestamp': datetime.datetime(
                2000, 8, 1).strftime('%Y-%m-%dT%H:%M:%S.%fZ')}
        pod = pytest.Mock(cluster=cluster)
        container = kube.Container(pod, {'containerID': 'docker://foo'})
        update = entityd.entityupdate.EntityUpdate('Entity')
        cluster.nodes = [
            kube.NodeItem(cluster, {'metadata': {'name': 'node'}})]
        cluster.proxy.get.return_value = {'/foo': [point_data]}
        kubernetes.container_metrics(container, update)
        assert not metrics[0].called
        assert not metrics[1].called
        assert not metrics[2].called

    def test_no_points(self, cluster, metrics, loghandler):
        pod = pytest.Mock(cluster=cluster)
        container = kube.Container(pod, {'containerID': 'docker://foo'})
        update = entityd.entityupdate.EntityUpdate('Entity')
        cluster.nodes = [
            kube.NodeItem(cluster, {'metadata': {'name': 'node'}})]
        cluster.proxy.get.return_value = {}
        kubernetes.container_metrics(container, update)
        assert loghandler.has_warning(
            'No points given for container with ID foo')
        assert not metrics[0].called
        assert not metrics[1].called
        assert not metrics[2].called
