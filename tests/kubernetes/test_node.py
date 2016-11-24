import datetime
import types

import cobe
import kube
import pytest
import requests

import entityd.kubernetes.node


@pytest.fixture
def cluster(request):
    """Mock of ``kube.Cluster`` of 2 nodes, one having 2 pods, the other 1."""
    cluster = kube.Cluster()
    request.addfinalizer(cluster.close)
    nodes = [
        kube.NodeItem(cluster, {
                'metadata': {
                    'name': 'nodename1',
                    'resourceVersion': '12903054',
                    'creationTimestamp': '2016-10-03T12:49:32Z',
                    'selfLink': '/api/v1/nodes/nodename1',
                    'uid': '7b211c2e-9644-11e6-8a78-42010af00021',
                },
                'status': {
                    'nodeInfo': {
                        'bootID':
                            'd4e0c0ae-290c-4e79-ae78-88b5d6cf215b',
                }}}),
        kube.NodeItem(cluster, {
                'metadata': {
                    'name': 'nodename2',
                    'resourceVersion': '12503032',
                    'creationTimestamp': '2016-10-02T15:32:21Z',
                    'selfLink': '/api/v1/nodes/nodename2',
                    'uid': '7895566a-9644-11e6-8a78-42010af00021',
                },
                'status': {
                    'nodeInfo': {
                        'bootID':
                            'f5c1d4bf-173f-5c51-bf32-97c4e2eg123e',
                }}})]
    pods = [
        kube.PodItem(cluster, {
            'metadata': {
                'name': 'podname1-v3-ut4bz',
                'namespace': 'namespace1',
            },
            'spec': {
                'nodeName': 'nodename1'}}),
        kube.PodItem(cluster, {
                'metadata': {
                    'name': 'podname2-v3-xa5at',
                    'namespace': 'namespace2',
                },
                'spec': {
                    'nodeName': 'nodename1'}}),
        kube.PodItem(cluster, {
                'metadata': {
                    'name': 'podname3-v1-ab2yx',
                    'namespace': 'namespace3',
                },
                'spec': {
                    'nodeName': 'nodename2'}})]
    mock_cluster = types.SimpleNamespace(nodes=nodes, pods=pods)
    return mock_cluster


@pytest.fixture
def cluster_missing_node(request):
    """"Mock of ``kube.Cluster`` that includes a pod with no node info.

    In practice, this might be due to the pod not yet having been fully
    established within k8s; e.g. awaiting disks.
    """
    cluster = kube.Cluster()
    request.addfinalizer(cluster.close)
    nodes = [
        kube.NodeItem(cluster, {
                'metadata': {
                    'name': 'nodename1',
                    'resourceVersion': '12903054',
                    'creationTimestamp': '2016-10-03T12:49:32Z',
                    'selfLink': '/api/v1/nodes/nodename1',
                    'uid': '7b211c2e-9644-11e6-8a78-42010af00021',
                },
                'status': {
                    'nodeInfo': {
                        'bootID':
                            'd4e0c0ae-290c-4e79-ae78-88b5d6cf215b',
                }}})]
    pods = [
        kube.PodItem(cluster, {
                'metadata': {
                    'name': 'podname1-v3-ut4bz',
                    'namespace': 'namespace1',
                },
                'spec': {
                    'nodeName': 'nodename1'}}),
        kube.PodItem(cluster, {
                'metadata': {
                    'name': 'podname3-v1-ab2yx',
                    'namespace': 'namespace3',
                },
                'spec': {},
            })]
    mock_cluster = types.SimpleNamespace(nodes=nodes, pods=pods)
    return mock_cluster


@pytest.fixture
def node(request, pm, config, session):
    """Fixture providing instance of ``node.NodeEntity``."""
    node = entityd.kubernetes.node.NodeEntity()
    pm.register(node, name='entityd.kubernetes.node.NodeEntity')
    node.entityd_sessionstart(session)
    node.cluster.close()
    node.entityd_configure(config)
    return node


@pytest.fixture
def entities(node, monkeypatch, cluster):
    """Fixture providing entities."""
    node.cluster = cluster
    entities = node.entityd_find_entity(
        name='Kubernetes:Node', attrs=None, include_ondemand=False)
    return entities


@pytest.fixture
def entities_missing_nodename(node, monkeypatch, cluster_missing_node):
    """Fixture providing entities where pod doesn't have nodename assigned."""
    node.cluster = cluster_missing_node
    entities = node.entityd_find_entity(
        name='Kubernetes:Node', attrs=None, include_ondemand=False)
    return entities


def test_NodeEntity_has_kube_cluster_instance(node):
    assert isinstance(node.cluster, kube._cluster.Cluster)


def test_sessionfinish(monkeypatch, node):
    assert isinstance(node.cluster, kube._cluster.Cluster)
    mock = pytest.Mock()
    node.cluster = mock
    node.entityd_sessionfinish()
    mock.close.assert_called_once_with()


def test_find_entity_with_attrs_not_none(node):
    with pytest.raises(LookupError):
        node.entityd_find_entity(
            'Kubernetes:Node', {'attr': 'foo-entity-bar'})


def test_get_first_entity(entities, node):
    entity = next(entities)
    assert entity.metype == 'Host'
    assert entity.label == 'nodename1'
    assert entity.attrs.get(
        'bootid').value == 'd4e0c0ae-290c-4e79-ae78-88b5d6cf215b'
    assert entity.attrs.get('bootid').traits == {'entity:id'}
    assert entity.attrs.get('kubernetes:kind').value == 'Node'
    assert entity.attrs.get('meta:name').value == 'nodename1'
    assert entity.attrs.get('meta:version').value == '12903054'
    assert entity.attrs.get('meta:created').value == '2016-10-03T12:49:32Z'
    assert entity.attrs.get('meta:link').value == '/api/v1/nodes/nodename1'
    assert entity.attrs.get('meta:link').traits == {'uri'}
    assert entity.attrs.get(
        'meta:uid').value == '7b211c2e-9644-11e6-8a78-42010af00021'
    assert len(list(entity.children)) == 2
    assert cobe.UEID('340f1e6180ac0b158c07943bed281117') in entity.children
    assert cobe.UEID('ab38429878f52e48186876c283e7d9d6') in entity.children
    assert node._logged_k8s_unreachable == False


def test_get_second_entity(entities):
    _ = next(entities)
    entity = next(entities)
    assert entity.metype == 'Host'
    assert entity.label == 'nodename2'
    assert entity.attrs.get(
        'bootid').value == 'f5c1d4bf-173f-5c51-bf32-97c4e2eg123e'
    assert entity.attrs.get('bootid').traits == {'entity:id'}
    assert entity.attrs.get('kubernetes:kind').value == 'Node'
    assert len(list(entity.children)) == 1
    assert cobe.UEID('38ccc5c6b87e52e6debbbc5b344508c5') in entity.children


def test_get_entities_with_pod_missing_nodename(entities_missing_nodename):
    entity = next(entities_missing_nodename)
    assert entity.label == 'nodename1'
    assert len(list(entity.children)) == 1
    assert cobe.UEID('340f1e6180ac0b158c07943bed281117') in entity.children
    with pytest.raises(StopIteration):
        next(entities_missing_nodename)


def test_k8s_unreachable(node, monkeypatch):
    monkeypatch.setattr(entityd.kubernetes.node.NodeEntity,
                        'determine_pods_on_nodes',
                        pytest.Mock(side_effect=requests.ConnectionError))
    assert node._logged_k8s_unreachable == False
    assert list(node.entityd_find_entity(
        name='Kubernetes:Node', attrs=None, include_ondemand=False)) == []
    assert node._logged_k8s_unreachable == True
