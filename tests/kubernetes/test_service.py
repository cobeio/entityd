import types

import cobe
import kube
import pytest
import requests

import entityd.kubernetes.cluster
import entityd.kubernetes.service
import entityd.pm


@pytest.fixture
def cluster():
    """Mock of ``kube.Cluster`` with Service having 2 child pods."""
    services = [
        kube.ServiceItem(cluster, {
            'metadata': {
                'name': 'test_service',
                'namespace': 'test_namespace',
                'resourceVersion': '12903054',
                'creationTimestamp': '2016-10-03T12:49:32Z',
                'selfLink': 'test_link_path',
                'uid': '7b211c2e-9644-11e6-8a78-42010af00021',
                'labels': {'label1': 'string1',
                           'label2': 'string2'}}})
    ]
    pods = [
        kube.PodItem(cluster, {
            'metadata': {
                'name': 'podname1-v3-ut4bz',
                'namespace': 'namespace',
                'labels': {'label1': 'string1',
                           'label2': 'string2',
                           'label3': 'string3'},
            },
            'spec': {
                'nodeName': 'nodename1'}}),
        kube.PodItem(cluster, {
            'metadata': {
                'name': 'podname2-v3-xa5at',
                'namespace': 'namespace',
                'labels': {'label1': 'string1',
                           'label2': 'string2',
                           'label4': 'string4'},
            },
            'spec': {
                'nodeName': 'nodename1'}}),
        kube.PodItem(cluster, {
            'metadata': {
                'name': 'podname3-v3-tv9zw',
                'namespace': 'namespace',
                'labels': {'label5': 'string5',
                           'label6': 'string6'},
            },
            'spec': {
                'nodeName': 'nodename1'}}),
    ]
    kind = 'Kind.Service'
    return types.SimpleNamespace(services=services, pods=pods, kind=kind)


@pytest.fixture
def service(cluster_entity_plugin, pm, config, session):    # pylint: disable=unused-argument
    """Fixture providing instance of ``service.ServiceEntity``."""
    service = entityd.kubernetes.service.ServiceEntity()
    pm.register(service, name='entityd.kubernetes.service.ServiceEntity')
    service.entityd_sessionstart(session)
    service.entityd_configure(config)
    return service


@pytest.fixture
def entities(service, cluster):
    """Fixture providing entities."""
    service.cluster = cluster
    entities = service.entityd_find_entity(
        name='Kubernetes:Service', attrs=None, include_ondemand=False)
    return entities


def test_sessionfinish(service):
    assert isinstance(service.cluster, kube._cluster.Cluster)
    mock = pytest.Mock()
    service.cluster = mock
    service.entityd_sessionfinish()
    mock.close.assert_called_once_with()


def test_find_entity_with_attrs_not_none(service):
    with pytest.raises(LookupError):
        service.entityd_find_entity(
            'Kubernetes:Service', {'attr': 'foo-entity-bar'})


def test_service_entities(service, entities):
    entity = next(entities)
    assert entity.metype == 'Kubernetes:Service'
    assert entity.label == 'test_service'
    assert entity.attrs.get('kubernetes:kind').value == 'Service'
    assert entity.attrs.get('kubernetes:meta:name').value == 'test_service'
    assert entity.attrs.get('kubernetes:meta:version').value == '12903054'
    assert entity.attrs.get(
        'kubernetes:meta:created').value == '2016-10-03T12:49:32Z'
    assert entity.attrs.get('kubernetes:meta:link').value == 'test_link_path'
    assert entity.attrs.get('kubernetes:meta:link').traits == {'uri'}
    assert entity.attrs.get(
        'kubernetes:meta:uid').value == '7b211c2e-9644-11e6-8a78-42010af00021'
    assert len(list(entity.children)) == 2
    assert len(list(entity.parents)) == 1
    assert cobe.UEID('ff290adeb112ae377e8fca009ca4fd9f') in entity.parents
    assert cobe.UEID('5a3a32ba5409a19c744633eb9f81a222') in entity.children
    assert cobe.UEID('43043ccc3b2dc9f57abc64b052e6aa3e') in entity.children
    with pytest.raises(StopIteration):
        next(entities)
    assert service._logged_k8s_unreachable is False


def test_k8s_unreachable(service, monkeypatch):
    monkeypatch.setattr(service, 'create_entity',
                        pytest.Mock(side_effect=requests.ConnectionError))
    assert service._logged_k8s_unreachable is None
    assert list(service.entityd_find_entity(
        name='Kubernetes:Service', attrs=None, include_ondemand=False)) == []
    assert service._logged_k8s_unreachable is True


def test_no_cluster_ueid_found(session):
    serviceentity = entityd.kubernetes.service.ServiceEntity()
    serviceentity.entityd_sessionstart(session)
    def entityd_find_entity(name, attrs):    # pylint: disable=unused-argument
        return [[]]
    hooks = types.SimpleNamespace(entityd_find_entity=entityd_find_entity)
    pluginmanager = types.SimpleNamespace(hooks=hooks)
    serviceentity._session = types.SimpleNamespace(
        pluginmanager=pluginmanager)
    with pytest.raises(LookupError):
        assert serviceentity.cluster_ueid
