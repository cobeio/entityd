import collections
import datetime
import socket

import kube
import pytest
import requests

import entityd.entityupdate
import entityd.kubernetes


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
        entityd.kubernetes, 'apply_meta_update', pytest.Mock())
    return entityd.kubernetes.apply_meta_update


def test_entityd_configure(pm, config):
    plugin = pm.register(entityd.kubernetes)
    entityd.kubernetes.entityd_configure(config)
    assert set(config.entities.keys()) == set((
        'Kubernetes:Container',
        'Kubernetes:Pod',
        'Kubernetes:Namespace',
    ))
    for entity_plugin in config.entities.values():
        assert entity_plugin is plugin


class TestFindEntity:

    @pytest.fixture
    def generate_updates(self, monkeypatch):
        monkeypatch.setattr(entityd.kubernetes,
                            'generate_updates', pytest.Mock())
        return entityd.kubernetes.generate_updates

    @pytest.mark.parametrize(
        ('type_', 'generator_function'),
        entityd.kubernetes.ENTITIES_PROVIDED.items(),
    )
    def test(self, generate_updates, type_, generator_function):
        generator = entityd.kubernetes.entityd_find_entity(type_)
        assert generator is generate_updates.return_value
        assert generate_updates.call_args[0] == (
            getattr(entityd.kubernetes, generator_function),)

    def test_not_provided(self):
        assert entityd.kubernetes.entityd_find_entity('Foo') is None

    @pytest.mark.parametrize(
        'type_',
        entityd.kubernetes.ENTITIES_PROVIDED.keys(),
    )
    def test_attrs(self, type_):
        with pytest.raises(LookupError):
            entityd.kubernetes.entityd_find_entity(
                type_, {'meta:name': 'foo-entity-bar'})


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
    'update_generator', entityd.kubernetes.ENTITIES_PROVIDED.values())
def test_cluster_unreachable(unreachable_cluster, update_generator):  # pylint: disable=unused-argument
    generator = entityd.kubernetes.generate_updates(
        getattr(entityd.kubernetes, update_generator))
    assert list(generator) == []


@pytest.mark.parametrize(
    'update_generator', entityd.kubernetes.ENTITIES_PROVIDED.values())
def test_uncaught_status_error(monkeypatch, update_generator):

    def generator(_):
        print('initial')
        update = yield
        update.label = 'first'
        update = yield
        update.label = 'second'
        raise kube.StatusError
        update = yield
        update.label = 'third'

    generator.__name__ = update_generator
    generator = entityd.kubernetes.generate_updates(generator)
    updates = list(generator)
    assert len(updates) == 1
    assert updates[0].label == 'first'


class TestApplyMetaUpdate:

    def test(self):
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
        entityd.kubernetes.apply_meta_update(meta, update)
        assert update.attrs.get('meta:name').value == 'star'
        assert update.attrs.get('meta:name').traits == {'entity:id'}
        assert update.attrs.get('meta:namespace').value == 'andromeda'
        assert update.attrs.get('meta:namespace').traits == {'entity:id'}
        assert update.attrs.get('meta:version').value == '1234'
        assert update.attrs.get('meta:version').traits == set()
        assert update.attrs.get('meta:created').value == '2015-01-14T17:01:37Z'
        assert update.attrs.get('meta:created').traits == {'chrono:rfc3339'}
        assert update.attrs.get('meta:link').value == (
            '/api/v1/namespaces/andromeda/pods/star')
        assert update.attrs.get('meta:link').traits == {'uri'}
        assert update.attrs.get('meta:uid').value == (
            '7955593e-bae0-11e5-b0b9-42010af00091')
        assert update.attrs.get('meta:uid').traits == set()

    def test_missing_namespace(self):
        meta = kube.ObjectMeta(pytest.Mock(raw={
            'metadata': {
                'name': 'star',
                'resourceVersion': '1234',
                'creationTimestamp': '2015-01-14T17:01:37Z',
                'selfLink': '/api/v1/namespaces/andromeda/pods/star',
                'uid': '7955593e-bae0-11e5-b0b9-42010af00091',
            },
        }))
        update = entityd.entityupdate.EntityUpdate('Foo')
        entityd.kubernetes.apply_meta_update(meta, update)
        assert {attribute.name for attribute in update.attrs} == {
            'meta:name',
            'meta:version',
            'meta:created',
            'meta:link',
            'meta:uid',
        }


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
        cluster.namespaces.__iter__.return_value = iter(namespace_resources)
        namespaces = list(
            entityd.kubernetes.entityd_find_entity('Kubernetes:Namespace'))
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
            namespace_resources[0].meta, namespaces[0])
        assert meta_update.call_args_list[1][0] == (
            namespace_resources[1].meta, namespaces[1])


class TestPods:

    @pytest.fixture
    def namespaces(self, monkeypatch, cluster, meta_update):  # pylint: disable=unused-argument
        updates = []

        def generate_namespaces(cluster):  # pylint: disable=unused-argument
            update = yield
            update.attrs.set('meta:name', 'andromeda')
            updates.append(update)

        monkeypatch.setattr(
            entityd.kubernetes, 'generate_namespaces', generate_namespaces)
        return updates

    def test(self, cluster, meta_update, namespaces):
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
        pods = list(entityd.kubernetes.entityd_find_entity('Kubernetes:Pod'))
        assert len(pods) == 2
        assert pods[0].metype == 'Kubernetes:Pod'
        assert pods[0].label == 'pod-1'
        assert pods[0].attrs.get('phase').value == 'Running'
        assert pods[0].attrs.get('phase').traits == {'kubernetes:pod-phase'}
        assert pods[0].attrs.get('start_time').value == '2015-01-14T17:01:37Z'
        assert pods[0].attrs.get('start_time').traits == {'chrono:rfc3339'}
        assert pods[0].attrs.get('ip').value == '10.120.0.5'
        assert pods[0].attrs.get('ip').traits == {'ipaddr:v4'}
        assert list(pods[0].parents) == [namespaces[0].ueid]
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
            pod_resources[0].meta, pods[0])
        assert meta_update.call_args_list[1][0] == (
            pod_resources[1].meta, pods[1])

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
        pods = list(entityd.kubernetes.entityd_find_entity('Kubernetes:Pod'))
        assert len(pods) == 1
        assert pods[0].metype == 'Kubernetes:Pod'
        assert pods[0].label == 'pod-1'
        assert pods[0].attrs.get('message').value == 'Once upon a time ...'
        assert pods[0].attrs.get('message').traits == set()
        assert meta_update.call_count == 1
        assert meta_update.call_args_list[0][0] == (
            pod_resources[0].meta, pods[0])

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
        pods = list(entityd.kubernetes.entityd_find_entity('Kubernetes:Pod'))
        assert len(pods) == 1
        assert pods[0].metype == 'Kubernetes:Pod'
        assert pods[0].label == 'pod-1'
        assert pods[0].attrs.get('reason').value == 'ItsWorking'
        assert pods[0].attrs.get('reason').traits == set()
        assert meta_update.call_count == 1
        assert meta_update.call_args_list[0][0] == (
            pod_resources[0].meta, pods[0])

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
        pods = list(entityd.kubernetes.entityd_find_entity('Kubernetes:Pod'))
        assert len(pods) == 1
        assert pods[0].metype == 'Kubernetes:Pod'
        assert pods[0].label == 'pod-1'
        assert pods[0].attrs.get('ip').value == '2001:db8::8:800:200c:417a'
        assert pods[0].attrs.get('ip').traits == {'ipaddr:v6'}
        assert meta_update.call_count == 1
        assert meta_update.call_args_list[0][0] == (
            pod_resources[0].meta, pods[0])


class TestContainers:

    @pytest.fixture
    def raw_pod_resource(self):
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
                    },
                ],
            },
        }

    def test(self, cluster, raw_pod_resource):
        pod = kube.PodItem(cluster, raw_pod_resource)
        cluster.pods.__iter__.return_value = iter([pod])
        cluster.pods.fetch.return_value = pod
        containers = list(
            entityd.kubernetes.entityd_find_entity('Kubernetes:Container'))
        assert len(containers) == 1
        assert containers[0].metype == 'Kubernetes:Container'
        assert containers[0].label == 'container-1'
        assert containers[0].attrs.get('id').value == (
            'docker://3a542701e9896f6a4e526cc69e6'
            '191b221cf29e1cabb43edf3b47fe5b33a7a59')
        assert containers[0].attrs.get('id').traits == {'entity:id'}
        assert containers[0].attrs.get('name').value == 'container-1'
        assert containers[0].attrs.get('name').traits == {'entity:id'}
        assert containers[0].attrs.get('ready').value is True
        assert containers[0].attrs.get('ready').traits == set()
        assert containers[0].attrs.get('image:id').value == (
            'docker://33688d2af35f810373734d5928f'
            '3e7c579e2569aa80ed80580436f1fd90e53c6')
        assert containers[0].attrs.get('image:id').traits == set()
        assert containers[0].attrs.get('image:name').value == (
            'repository/user/image:tag')
        assert containers[0].attrs.get('image:name').traits == set()

    def test_running(self, cluster, raw_pod_resource):
        raw_pod_resource['status']['containerStatuses'][0]['state'] = {
            'running': {
                'startedAt': '2015-12-04T19:15:23Z',
            }
        }
        pod = kube.PodItem(cluster, raw_pod_resource)
        cluster.pods.__iter__.return_value = iter([pod])
        cluster.pods.fetch.return_value = pod
        container = list(
            entityd.kubernetes.entityd_find_entity('Kubernetes:Container'))[0]
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
        container = list(
            entityd.kubernetes.entityd_find_entity('Kubernetes:Container'))[0]
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
        container = list(
            entityd.kubernetes.entityd_find_entity('Kubernetes:Container'))[0]
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

    def test_missing_namespace(self, cluster, raw_pod_resource):
        pod = kube.PodItem(cluster, raw_pod_resource)
        cluster.pods.__iter__.return_value = iter([pod])
        cluster.namespaces.fetch.side_effect = LookupError
        containers = list(
            entityd.kubernetes.entityd_find_entity('Kubernetes:Container'))
        assert not containers

    def test_missing_pod(self, cluster, raw_pod_resource):
        pod = kube.PodItem(cluster, raw_pod_resource)
        cluster.pods.__iter__.return_value = iter([pod])
        mock_namespace = cluster.namespaces.fetch.return_value
        mock_namespace.pods.fetch.side_effect = LookupError
        containers = list(
            entityd.kubernetes.entityd_find_entity('Kubernetes:Container'))
        assert not containers


class TestMetric:

    def test_repr(self):
        metric = entityd.kubernetes.Metric('foo', ('A', 'B'), {'spam', 'eggs'})
        assert repr(metric) == '<Metric foo @ A.B traits: eggs, spam>'

    def test(self):
        metric = entityd.kubernetes.Metric('foo', ('A', 'B'), {'trait'})
        update = entityd.entityupdate.EntityUpdate('Entity')
        object_ = {'A': {'B': 'value'}}
        metric.apply(object_, update)
        attribute = update.attrs.get('foo')
        assert attribute.name == 'foo'
        assert attribute.value is object_['A']['B']
        assert attribute.traits == {'trait'}

    def test_unresolvable(self):
        metric = entityd.kubernetes.Metric('foo', ('A', 'B'), {'trait'})
        update = entityd.entityupdate.EntityUpdate('Entity')
        object_ = {'A': {'C': 'value'}}
        metric.apply(object_, update)
        assert update.attrs.deleted() == {'foo'}
        with pytest.raises(KeyError):
            update.attrs.get('foo')

    def test_with_prefix(self):
        metric = entityd.kubernetes.Metric('bar', ('B', 'C'), {'trait'})
        update = entityd.entityupdate.EntityUpdate('Entity')
        object_ = {'A': {'B': {'C': 'value'}}}
        metric.with_prefix('foo', ('A',)).apply(object_, update)
        attribute = update.attrs.get('foo:bar')
        assert attribute.name == 'foo:bar'
        assert attribute.value is object_['A']['B']['C']
        assert attribute.traits == {'trait'}

    def test_nanosecond(self):
        metric = entityd.kubernetes.NanosecondMetric('foo', ('A',), set())
        update = entityd.entityupdate.EntityUpdate('Entity')
        object_ = {'A': 123000000000}
        metric.apply(object_, update)
        attribute = update.attrs.get('foo')
        assert attribute.name == 'foo'
        assert attribute.value == 123
        assert attribute.traits == set()

    def test_millisecond(self):
        metric = entityd.kubernetes.MillisecondMetric('foo', ('A',), set())
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
        points = [entityd.kubernetes.Point(t, {}) for t in (
            target + datetime.timedelta(seconds=-1),
            target + datetime.timedelta(seconds=0),
            target + datetime.timedelta(seconds=1),
        )]
        point = entityd.kubernetes.select_nearest_point(target, points, 5.0)
        assert point is points[1]

    def test_no_points(self):
        target = datetime.datetime(2000, 8, 1)
        with pytest.raises(ValueError):
            entityd.kubernetes.select_nearest_point(target, [], 5.0)

    @pytest.mark.parametrize('delta', [-20.1 * 60, 20.1 * 60])
    def test_exceed_threshold(self, delta):
        target = datetime.datetime(2000, 8, 1)
        points = [entityd.kubernetes.Point(
            target + datetime.timedelta(seconds=delta), {})]
        with pytest.raises(ValueError):
            entityd.kubernetes.select_nearest_point(target, points,
                                                    threshold=20 * 60)

    @pytest.mark.parametrize('threshold', [1, 5, 10, 20])
    def test_threshold_used_ok(self, threshold):
        target = datetime.datetime(2000, 8, 1)
        points = [
            entityd.kubernetes.Point(
                target + datetime.timedelta(seconds=threshold), {})
        ]
        point = entityd.kubernetes.select_nearest_point(target, points,
                                                        threshold=threshold)
        assert point == points[0]

    @pytest.mark.parametrize('threshold', [1, 5, 10, 20])
    def test_threshold_used_exceed(self, threshold):
        target = datetime.datetime(2000, 8, 1)
        points = [
            entityd.kubernetes.Point(
                target + datetime.timedelta(seconds=threshold + 1), {})
        ]
        with pytest.raises(ValueError):
            entityd.kubernetes.select_nearest_point(target, points,
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
        points = entityd.kubernetes.cadvisor_to_points(raw_points)
        assert len(points) == 1
        assert points[0].timestamp == datetime.datetime(2000, 8, 1, 12)
        assert points[0].data is raw_points[0]

    def test_invalid_offset_separator(self):
        raw_points = [
            {'timestamp': '2000-08-01T10:00:00.0Z'},
            {'timestamp': '2000-08-01T12:00:00.0#00:00'},
            {'timestamp': '2000-08-01T14:00:00.0Z'},
        ]
        points = entityd.kubernetes.cadvisor_to_points(raw_points)
        assert len(points) == 2
        assert points[0].timestamp == datetime.datetime(2000, 8, 1, 10)
        assert points[0].data is raw_points[0]
        assert points[1].timestamp == datetime.datetime(2000, 8, 1, 14)
        assert points[1].data is raw_points[2]

class TestSimpleMetrics:

    def test(self, monkeypatch):
        point = entityd.kubernetes.Point(pytest.Mock(), {
            'foo': 50,
            'bar': 100,
        })
        monkeypatch.setattr(
            entityd.kubernetes,
            'METRICS_CONTAINER',
            [
                entityd.kubernetes.Metric('foo', ('foo',), set()),
                entityd.kubernetes.Metric('bar', ('bar',), set()),
            ],
        )
        update = entityd.entityupdate.EntityUpdate('Entity')
        entityd.kubernetes.simple_metrics(point, update)
        attribute_foo = update.attrs.get('foo')
        attribute_bar = update.attrs.get('bar')
        assert attribute_foo.value == 50
        assert attribute_foo.traits == set()
        assert attribute_bar.value == 100
        assert attribute_bar.traits == set()


class TestFileSystemMetrics:

    def test(self, monkeypatch):
        point = entityd.kubernetes.Point(pytest.Mock(), {
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
            entityd.kubernetes,
            'METRICS_FILESYSTEM',
            [entityd.kubernetes.Metric('attribute', ('spam',), set())],
        )
        update = entityd.entityupdate.EntityUpdate('Entity')
        entityd.kubernetes.filesystem_metrics(point, update)
        attribute_foo = update.attrs.get('filesystem:foo:attribute')
        attribute_bar = update.attrs.get('filesystem:bar:attribute')
        assert attribute_foo.value == 50
        assert attribute_foo.traits == set()
        assert attribute_bar.value == 100
        assert attribute_bar.traits == set()

    def test_no_filesystem_field(self):
        point = entityd.kubernetes.Point(pytest.Mock(), {})
        update = entityd.entityupdate.EntityUpdate('Entity')
        entityd.kubernetes.filesystem_metrics(point, update)
        assert len(list(update.attrs)) == 0


class TestDiskIOMetrics:

    def test(self, monkeypatch):
        point = entityd.kubernetes.Point(pytest.Mock(), {
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
            entityd.kubernetes,
            'METRICS_DISKIO',
            {'foo': [entityd.kubernetes.Metric(
                'attribute', ('stats', 'value',), set())]},
        )
        update = entityd.entityupdate.EntityUpdate('Entity')
        entityd.kubernetes.diskio_metrics(point, update)
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
            ('foo', [entityd.kubernetes.Metric(
                'foo', ('stats', 'value'), set())]),
            ('bar', [entityd.kubernetes.Metric(
                'bar', ('stats', 'value'), set())]),
        ])
        point = entityd.kubernetes.Point(pytest.Mock(), {
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
        monkeypatch.setattr(entityd.kubernetes, 'METRICS_DISKIO', metrics)
        update = entityd.entityupdate.EntityUpdate('Entity')
        entityd.kubernetes.diskio_metrics(point, update)
        with pytest.raises(KeyError):
            update.attrs.get('io:8:0:foo')
        attribute = update.attrs.get('io:8:0:bar')
        assert attribute.value == 50
        assert attribute.traits == set()


class TestContainerMetrics:

    @pytest.fixture()
    def metrics(self, monkeypatch):
        monkeypatch.setattr(entityd.kubernetes,
                            'simple_metrics', pytest.Mock())
        monkeypatch.setattr(entityd.kubernetes,
                            'filesystem_metrics', pytest.Mock())
        monkeypatch.setattr(entityd.kubernetes,
                            'diskio_metrics', pytest.Mock())
        return (entityd.kubernetes.simple_metrics,
                entityd.kubernetes.filesystem_metrics,
                entityd.kubernetes.diskio_metrics)

    def test(self, cluster, metrics):
        point_data = {
            'timestamp':
                datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')}
        pod = pytest.Mock(cluster=cluster)
        container = kube.Container(pod, {'containerID': 'docker://foo'})
        update = entityd.entityupdate.EntityUpdate('Entity')
        cluster.nodes = [
            kube.NodeItem(cluster, {'metadata': {'name': 'node'}})]
        cluster.proxy.get.return_value = {'/foo': [point_data]}
        entityd.kubernetes.container_metrics(container, update)
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
        entityd.kubernetes.container_metrics(container, update)
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
        entityd.kubernetes.container_metrics(container, update)
        assert not metrics[0].called
        assert not metrics[1].called
        assert not metrics[2].called
