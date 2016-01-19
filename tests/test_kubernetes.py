import kube
import pytest

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
        entityd.kubernetes, '_apply_meta_update', pytest.Mock())
    return entityd.kubernetes._apply_meta_update


class TestPods:

    def test(self, cluster, meta_update):
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
        assert pods[0].attrs.get('start_time').value == '2015-01-14 17:01:37'
        assert pods[0].attrs.get('start_time').type == 'chrono:rfc3339'
        assert pods[0].attrs.get('ip').value == '10.120.0.5'
        assert pods[0].attrs.get('ip').type == 'ip:v4'
        assert pods[1].metype == 'Kubernetes:Pod'
        assert pods[1].label == 'pod-2'
        assert pods[1].attrs.get('phase').value == 'Running'
        assert pods[1].attrs.get('phase').type == 'kubernetes:pod-phase'
        assert pods[1].attrs.get('start_time').value == '2016-01-14 17:01:37'
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
