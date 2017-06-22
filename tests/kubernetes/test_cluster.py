import types

import kube
import pytest
import requests

import entityd.kubernetes.cluster
import entityd.pm


@pytest.yield_fixture
def clusterentity():
    """Instance of ``cluster.ClusterEntity``."""
    clusterentity = entityd.kubernetes.cluster.ClusterEntity()
    session = pytest.Mock()
    clusterentity.entityd_sessionstart(session)
    yield clusterentity
    clusterentity.entityd_sessionfinish()


@pytest.fixture(params=[{'providerID': 'gce://Cluster-clustername/'
                                       'europe-west1-d/'
                                       'gke-cobetest-60058af9-node-3fqh'},
                        {'externalID': 'clustername'}])
def kube_mock(monkeypatch, request):
    """Mocking out of kube."""
    class Spec:
        @staticmethod
        def spec():
            return request.param
    nodes = [Spec]
    addresses = {
        'metadata': {
            'name': 'kubernetes',
            },
        'subsets': [
            {
                'addresses': [
                    {
                        'ip': '69.69.69.69',
                        },
                    ],
                'ports': [
                    {
                        'name': 'https',
                        'port': 443,
                        }]}]}
    proxy = types.SimpleNamespace(get=lambda address: addresses)
    monkeypatch.setattr(kube, 'Cluster', lambda: types.SimpleNamespace(
        nodes=nodes, proxy=proxy, close=lambda: 1))


@pytest.fixture(params=[[], [{}], 'TypeError'])
def kube_mock_bad_format(monkeypatch, request):
    """Mocking out of kube where kubernetes provides unexpected data format."""
    addresses = {
        'metadata': {
            'name': 'kubernetes',
        },
        'subsets': [
            {
                'addresses': request.param,
                'ports': [
                    {
                        'name': 'https',
                        'port': 443,
                        }]}]}
    proxy = types.SimpleNamespace(get=lambda address: addresses)
    monkeypatch.setattr(kube, 'Cluster', lambda: types.SimpleNamespace(
        proxy=proxy, close=lambda: 1))


@pytest.yield_fixture
def clusterentity_mocked(kube_mock, clusterentity):  # pylint: disable=unused-argument
    """Instance of ``cluster.ClusterEntity``, applying the kube mocking."""
    yield clusterentity


@pytest.yield_fixture
def clusterentity_no_address(kube_mock_bad_format, clusterentity):  # pylint: disable=unused-argument
    """Instance of ``cluster.ClusterEntity``; address not findable."""
    yield clusterentity


def test_ClusterEntity_has_kube_cluster_instance(clusterentity):
    assert isinstance(clusterentity._cluster, kube._cluster.Cluster)


def test_configure():
    config = pytest.Mock()
    entityd.kubernetes.cluster.ClusterEntity.entityd_configure(config)
    assert config.addentity.called_once_with(
        'Kubernetes:Cluster', 'entityd.kubernetes.cluster.ClusterEntity')


def test_find_entity_with_attrs_not_none(clusterentity):
    with pytest.raises(LookupError):
        clusterentity.entityd_find_entity(
            'Kubernetes:Cluster', {'attr': 'foo-entity-bar'})


def test_find_entity(clusterentity_mocked):
    entities = clusterentity_mocked.entityd_find_entity(
        'Kubernetes:Cluster', None, include_ondemand=False)
    entity = next(entities)
    assert entity.metype == 'Kubernetes:Cluster'
    assert entity.label == 'Cluster-clustername'
    assert entity.attrs.get('kubernetes:kind').value == 'Cluster'
    assert entity.attrs.get(
        'kubernetes:cluster').value == 'https://69.69.69.69:443/'
    assert entity.attrs.get(
        'kubernetes:cluster').traits == {'entity:id'}
    with pytest.raises(StopIteration):
        next(entities)
    assert clusterentity_mocked._logged_k8s_unreachable is False


def test_k8s_unreachable_log(clusterentity, monkeypatch, loghandler):
    assert clusterentity._logged_k8s_unreachable is False
    def create_entity():
        raise requests.ConnectionError
    monkeypatch.setattr(clusterentity, 'create_entity', create_entity)
    with pytest.raises(StopIteration):
        next(clusterentity.find_cluster_entity())
    assert loghandler.has_info()
    assert clusterentity._logged_k8s_unreachable is True


def test_k8s_unreachable_no_log(clusterentity, monkeypatch, loghandler):
    monkeypatch.setattr(clusterentity, '_logged_k8s_unreachable', True)
    def create_entity():
        raise requests.ConnectionError
    monkeypatch.setattr(clusterentity, 'create_entity', create_entity)
    with pytest.raises(StopIteration):
        next(clusterentity.find_cluster_entity())
    assert not loghandler.has_info()
    assert clusterentity._logged_k8s_unreachable is True


def test_address_cached(clusterentity):
    clusterentity._address = 'testaddress'
    assert clusterentity.address == 'testaddress'


def test_address_exception(clusterentity_no_address, loghandler):
    assert clusterentity_no_address.address is None
    assert loghandler.has_error()


def test_sessionfinish(clusterentity):
    assert isinstance(clusterentity._cluster, kube._cluster.Cluster)
    mock = pytest.Mock()
    clusterentity._cluster = mock
    clusterentity.entityd_sessionfinish()
    mock.close.assert_called_once_with()
