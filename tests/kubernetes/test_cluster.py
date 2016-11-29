import types

import kube
import pytest

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


@pytest.fixture
def kube_mock(monkeypatch):
    """Mocking out of kube."""
    nodes = [
        types.SimpleNamespace(raw={
            'spec': {'providerID': 'gce://clustername/europe-west1-d'
                                   '/gke-cobetest-60058af9-node-3fqh'}})]
    addresses = {
        'items': [
            {
                'metadata': {
                    'name': 'kubernetes',
                },
                'subsets': [
                    {
                        'addresses': [
                            {
                                'ip': '69.69.69.69',
                            }]}]}]}
    proxy = types.SimpleNamespace(get=lambda address: addresses)
    monkeypatch.setattr(kube, 'Cluster', lambda: types.SimpleNamespace(
        nodes=nodes, proxy=proxy, close=lambda: 1))


@pytest.yield_fixture
def clusterentity_mocked(kube_mock, clusterentity):
    """Instance of ``cluster.ClusterEntity``, applying the kube mocking."""
    yield clusterentity


def test_ClusterEntity_has_kube_cluster_instance(clusterentity):
    assert isinstance(clusterentity._cluster, kube._cluster.Cluster)


def test_find_entity_with_attrs_not_none(clusterentity):
    with pytest.raises(LookupError):
        clusterentity.entityd_find_entity(
            'Kubernetes:Cluster', {'attr': 'foo-entity-bar'})


def test_get_entity(clusterentity_mocked):
    entities = clusterentity_mocked.entityd_find_entity(
        'Kubernetes:Cluster', None, include_ondemand=False)
    entity=next(entities)
    assert entity.metype == 'Kubernetes:Cluster'
    assert entity.label == 'clustername'
    assert entity.attrs.get('kubernetes:kind').value == 'Cluster'
    assert entity.attrs.get(
        'kubernetes:api_endpoint').value == '69.69.69.69'
    assert entity.attrs.get(
        'kubernetes:api_endpoint').traits == {'entity:id'}
    assert entity.attrs.get('kubernetes:name').value == 'clustername'
    assert clusterentity_mocked._logged_k8s_unreachable == False


def test_sessionfinish(clusterentity):
    assert isinstance(clusterentity._cluster, kube._cluster.Cluster)
    mock = pytest.Mock()
    clusterentity._cluster = mock
    clusterentity.entityd_sessionfinish()
    mock.close.assert_called_once_with()
