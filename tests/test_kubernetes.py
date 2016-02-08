import socket

import kube
import pytest

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


@pytest.fixture
def localhost_8001():
    """Check that nothing is listening on localhost:8001.

    If there is then the test will be failed.
    """
    try:
        with socket.create_connection(('localhost', 8001), 0.5):
            pytest.fail('Something is listening on localhost:8001')
    except ConnectionRefusedError:
        pass


@pytest.mark.parametrize(
    'update_generator', entityd.kubernetes.ENTITIES_PROVIDED.values())
def test_cluster_unreachable(localhost_8001, update_generator):
    generator = entityd.kubernetes.generate_updates(
        getattr(entityd.kubernetes, update_generator))
    assert list(generator) == []


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
        assert update.attrs.get('meta:name').type == 'id'
        assert update.attrs.get('meta:namespace').value == 'andromeda'
        assert update.attrs.get('meta:namespace').type == 'id'
        assert update.attrs.get('meta:version').value == '1234'
        assert update.attrs.get('meta:version').type is None
        assert update.attrs.get('meta:created').value == '2015-01-14T17:01:37Z'
        assert update.attrs.get('meta:created').type == 'chrono:rfc3339'
        assert update.attrs.get('meta:link').value == (
            '/api/v1/namespaces/andromeda/pods/star')
        assert update.attrs.get('meta:link').type == 'uri'
        assert update.attrs.get('meta:uid').value == (
            '7955593e-bae0-11e5-b0b9-42010af00091')
        assert update.attrs.get('meta:uid').type is None

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
            kube.NamespaceResource(cluster, {
                'metadata': {
                    'name': 'namespace-1',
                    'namespace': 'andromeda',
                },
                'status': {
                    'phase': 'Active',
                },
            }),
            kube.NamespaceResource(cluster, {
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
            'phase').type == 'kubernetes:namespace-phase'
        assert namespaces[1].metype == 'Kubernetes:Namespace'
        assert namespaces[1].label == 'namespace-2'
        assert namespaces[1].attrs.get('phase').value == 'Terminating'
        assert namespaces[1].attrs.get(
            'phase').type == 'kubernetes:namespace-phase'
        assert meta_update.call_count == 2
        assert meta_update.call_args_list[0][0] == (
            namespace_resources[0].meta, namespaces[0])
        assert meta_update.call_args_list[1][0] == (
            namespace_resources[1].meta, namespaces[1])


class TestPods:

    @pytest.fixture
    def namespaces(self, monkeypatch, cluster, meta_update):
        updates = []

        def generate_namespaces(cluster):
            update = yield
            update.attrs.set('meta:name', 'andromeda')
            updates.append(update)

        monkeypatch.setattr(
            entityd.kubernetes, 'generate_namespaces', generate_namespaces)
        return updates

    def test(self, cluster, meta_update, namespaces):
        pod_resources = [
            kube.PodResource(cluster, {
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
            kube.PodResource(cluster, {
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
        assert pods[0].attrs.get('phase').type == 'kubernetes:pod-phase'
        assert pods[0].attrs.get('start_time').value == '2015-01-14T17:01:37Z'
        assert pods[0].attrs.get('start_time').type == 'chrono:rfc3339'
        assert pods[0].attrs.get('ip').value == '10.120.0.5'
        assert pods[0].attrs.get('ip').type == 'ip:v4'
        assert list(pods[0].parents) == [namespaces[0].ueid]
        assert pods[1].metype == 'Kubernetes:Pod'
        assert pods[1].label == 'pod-2'
        assert pods[1].attrs.get('phase').value == 'Running'
        assert pods[1].attrs.get('phase').type == 'kubernetes:pod-phase'
        assert pods[1].attrs.get('start_time').value == '2016-01-14T17:01:37Z'
        assert pods[1].attrs.get('start_time').type == 'chrono:rfc3339'
        assert pods[1].attrs.get('ip').value == '10.120.0.7'
        assert pods[1].attrs.get('ip').type == 'ip:v4'
        assert meta_update.call_count == 2
        assert meta_update.call_args_list[0][0] == (
            pod_resources[0].meta, pods[0])
        assert meta_update.call_args_list[1][0] == (
            pod_resources[1].meta, pods[1])

    def test_with_message(self, cluster, meta_update):
        pod_resources = [
            kube.PodResource(cluster, {
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
        assert pods[0].attrs.get('message').type is None
        assert meta_update.call_count == 1
        assert meta_update.call_args_list[0][0] == (
            pod_resources[0].meta, pods[0])

    def test_with_reason(self, cluster, meta_update):
        pod_resources = [
            kube.PodResource(cluster, {
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
        assert pods[0].attrs.get('reason').type is None
        assert meta_update.call_count == 1
        assert meta_update.call_args_list[0][0] == (
            pod_resources[0].meta, pods[0])

    def test_ipv6(self, cluster, meta_update):
        pod_resources = [
            kube.PodResource(cluster, {
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
        assert pods[0].attrs.get('ip').type == 'ip:v6'
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

    def test(self, monkeypatch, cluster, raw_pod_resource):
        pod = kube.PodResource(cluster, raw_pod_resource)
        cluster.pods.__iter__.return_value = iter([pod])
        mock_namespace = cluster.namespaces.fetch.return_value
        mock_namespace.pods.fetch.return_value = pod
        containers = list(
            entityd.kubernetes.entityd_find_entity('Kubernetes:Container'))
        assert len(containers) == 1
        assert containers[0].metype == 'Kubernetes:Container'
        assert containers[0].label == 'container-1'
        assert containers[0].attrs.get('id').value == (
            '3a542701e9896f6a4e526cc69e6191b221cf29e1cabb43edf3b47fe5b33a7a59')
        assert containers[0].attrs.get('id').type == 'id'
        assert containers[0].attrs.get('name').value == 'container-1'
        assert containers[0].attrs.get('name').type == 'id'
        assert containers[0].attrs.get('ready').value is True
        assert containers[0].attrs.get('ready').type is None
        assert containers[0].attrs.get('image:id').value == (
            '33688d2af35f810373734d5928f3e7c579e2569aa80ed80580436f1fd90e53c6')
        assert containers[0].attrs.get('image:id').type is None
        assert containers[0].attrs.get('image:name').value == (
            'repository/user/image:tag')
        assert containers[0].attrs.get('image:name').type is None

    def test_running(self, monkeypatch, cluster, raw_pod_resource):
        raw_pod_resource['status']['containerStatuses'][0]['state'] = {
            'running': {
                'startedAt': '2015-12-04T19:15:23Z',
            }
        }
        pod = kube.PodResource(cluster, raw_pod_resource)
        cluster.pods.__iter__.return_value = iter([pod])
        mock_namespace = cluster.namespaces.fetch.return_value
        mock_namespace.pods.fetch.return_value = pod
        container = list(
            entityd.kubernetes.entityd_find_entity('Kubernetes:Container'))[0]
        assert container.attrs.get(
            'state:started-at').value == '2015-12-04T19:15:23Z'
        assert container.attrs.get(
            'state:started-at').type == 'chrono:rfc3339'
        assert container.attrs.deleted() == {
            'state:reason',
            'state:exit-code',
            'state:signal',
            'state:message',
            'state:finished-at',
        }

    def test_waiting(self, monkeypatch, cluster, raw_pod_resource):
        raw_pod_resource['status']['containerStatuses'][0]['state'] = {
            'waiting': {
                'reason': 'FooBar',
            }
        }
        pod = kube.PodResource(cluster, raw_pod_resource)
        cluster.pods.__iter__.return_value = iter([pod])
        mock_namespace = cluster.namespaces.fetch.return_value
        mock_namespace.pods.fetch.return_value = pod
        container = list(
            entityd.kubernetes.entityd_find_entity('Kubernetes:Container'))[0]
        assert container.attrs.get('state:reason').value == 'FooBar'
        assert container.attrs.get('state:reason').type is None
        assert container.attrs.deleted() == {
            'state:started-at',
            'state:exit-code',
            'state:signal',
            'state:message',
            'state:finished-at',
        }

    def test_terminated(self, monkeypatch, cluster, raw_pod_resource):
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
        pod = kube.PodResource(cluster, raw_pod_resource)
        cluster.pods.__iter__.return_value = iter([pod])
        mock_namespace = cluster.namespaces.fetch.return_value
        mock_namespace.pods.fetch.return_value = pod
        container = list(
            entityd.kubernetes.entityd_find_entity('Kubernetes:Container'))[0]
        assert container.attrs.get(
            'state:started-at').value == '2015-12-04T19:15:23Z'
        assert container.attrs.get(
            'state:started-at').type == 'chrono:rfc3339'
        assert container.attrs.get(
            'state:finished-at').value == '2016-12-04T19:15:23Z'
        assert container.attrs.get(
            'state:finished-at').type == 'chrono:rfc3339'
        assert container.attrs.get('state:reason').value == 'ItsDeadJim'
        assert container.attrs.get('state:reason').type is None
        assert container.attrs.get('state:exit-code').value == 0
        assert container.attrs.get('state:exit-code').type is None
        assert container.attrs.get('state:signal').value == 15
        assert container.attrs.get('state:signal').type is None
        assert container.attrs.get('state:message').value == '...'
        assert container.attrs.get('state:message').type is None
        assert container.attrs.deleted() == set()

    def test_missing_namespace(self, monkeypatch, cluster, raw_pod_resource):
        pod = kube.PodResource(cluster, raw_pod_resource)
        cluster.pods.__iter__.return_value = iter([pod])
        cluster.namespaces.fetch.side_effect = LookupError
        containers = list(
            entityd.kubernetes.entityd_find_entity('Kubernetes:Container'))
        assert not containers

    def test_missing_pod(self, monkeypatch, cluster, raw_pod_resource):
        pod = kube.PodResource(cluster, raw_pod_resource)
        cluster.pods.__iter__.return_value = iter([pod])
        mock_namespace = cluster.namespaces.fetch.return_value
        mock_namespace.pods.fetch.side_effect = LookupError
        containers = list(
            entityd.kubernetes.entityd_find_entity('Kubernetes:Container'))
        assert not containers
