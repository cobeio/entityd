import types

import cobe
import kube
import pytest
import requests

import entityd.kubernetes.cluster
import entityd.kubernetes.service
import entityd.pm

@pytest.fixture
def kube_services_ipv4():
    return [
        kube.ServiceItem(None, {
            'metadata': {
                'name': 'test_service',
                'namespace': 'test_namespace',
                'resourceVersion': '12903054',
                'creationTimestamp': '2016-10-03T12:49:32Z',
                'selfLink': 'test_link_path',
                'uid': '7b211c2e-9644-11e6-8a78-42010af00021',
                'labels': {'label1': 'string1',
                           'label2': 'string2'}},
            'spec': {
                'selector': {
                    'svc': 'test_service',
                    'ti': 'test-ti',
                },
            },
            'status': {
                'loadBalancer': {
                    'ingress': [
                        {'ip': '146.145.27.27'}]}}})]


@pytest.fixture
def kube_services_ipv6():
    return [
        kube.ServiceItem(None, {
            'metadata': {
                'name': 'test_service',
                'namespace': 'test_namespace',
                'resourceVersion': '12903054',
                'creationTimestamp': '2016-10-03T12:49:32Z',
                'selfLink': 'test_link_path',
                'uid': '7b211c2e-9644-11e6-8a78-42010af00021',
                'labels': {'label1': 'string1',
                           'label2': 'string2'}},
            'status': {
                'loadBalancer': {
                    'ingress': [
                        {'ip': '2001:db8:85a3::8a2e:370:7334'}]}}})]


@pytest.fixture
def kube_services_hostname():
    return [
        kube.ServiceItem(None, {
            'metadata': {
                'name': 'test_service',
                'namespace': 'test_namespace',
                'resourceVersion': '12903054',
                'creationTimestamp': '2016-10-03T12:49:32Z',
                'selfLink': 'test_link_path',
                'uid': '7b211c2e-9644-11e6-8a78-42010af00021',
                'labels': {'label1': 'string1',
                           'label2': 'string2'}},
            'status': {
                'loadBalancer': {
                    'ingress': [
                        {'hostname': 'derreck'}]}}})]


@pytest.fixture
def kube_pods():
    return {
        'items': [
            {
                'metadata': {
                    'name': 'podname1-v3-ut4bz',
                    'namespace': 'test_namespace',
                }
            },
            {
                'metadata': {
                    'name': 'podname2-v3-xa5at',
                    'namespace': 'test_namespace',
                },
            },
        ],
    }


@pytest.fixture
def cluster_ipv4(kube_services_ipv4, kube_pods):
    """Mock of ``kube.Cluster`` with service having IPv4 ingress point."""
    kind = 'Kind.Service'
    proxy = types.SimpleNamespace(get=pytest.Mock(return_value=kube_pods))
    pods = types.SimpleNamespace(api_path='api/v1')
    return types.SimpleNamespace(
        services=kube_services_ipv4, pods=pods, kind=kind, proxy=proxy)


@pytest.fixture
def cluster_ipv6(kube_services_ipv6, kube_pods):
    """Mock of ``kube.Cluster`` with service having IPv6 ingress point."""
    kind = 'Kind.Service'
    proxy = types.SimpleNamespace(get=pytest.Mock(return_value=kube_pods))
    pods = types.SimpleNamespace(api_path='api/v1')
    return types.SimpleNamespace(
        services=kube_services_ipv6, pods=pods, kind=kind, proxy=proxy)


@pytest.fixture
def cluster_hostname(kube_services_hostname, kube_pods):
    """Mock of ``kube.Cluster`` with service having hostname ingress point."""
    kind = 'Kind.Service'
    proxy = types.SimpleNamespace(get=pytest.Mock(return_value=kube_pods))
    pods = types.SimpleNamespace(api_path='api/v1')
    return types.SimpleNamespace(services=kube_services_hostname,
                                 pods=pods, kind=kind, proxy=proxy)


@pytest.fixture
def service(cluster_entity_plugin, pm, config, session):    # pylint: disable=unused-argument
    """Fixture providing instance of ``service.ServiceEntity``."""
    service = entityd.kubernetes.service.ServiceEntity()
    pm.register(service, name='entityd.kubernetes.service.ServiceEntity')
    service.entityd_sessionstart(session)
    service.entityd_configure(config)
    return service


@pytest.fixture
def entities_ipv4(service, cluster_ipv4):
    """Fixture providing entities for services with IPv4 ingress points."""
    service.cluster = cluster_ipv4
    entities = service.entityd_find_entity(
        name='Kubernetes:Service', attrs=None, include_ondemand=False)
    return entities


@pytest.fixture
def entities_ipv6(service, cluster_ipv6):
    """Fixture providing entities for services with IPv6 ingress points."""
    service.cluster = cluster_ipv6
    entities = service.entityd_find_entity(
        name='Kubernetes:Service', attrs=None, include_ondemand=False)
    return entities


@pytest.fixture
def entities_hostname(service, cluster_hostname):
    """Fixture providing entities for services with IPv4 ingress points."""
    service.cluster = cluster_hostname
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


def test_service_entities_ipv4(service, entities_ipv4, cluster_ipv4):
    # Test with service having IPv4 ingress point.
    entity = next(entities_ipv4)
    assert cluster_ipv4.proxy.get.call_args_list[0][1]['labelSelector'] in [
        'svc=test_service,ti=test-ti',
        'ti=test-ti,svc=test_service',
    ]
    assert cluster_ipv4.proxy.get.call_args_list[0][0] == (
        'api/v1/namespaces/test_namespace/pods',)
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
    assert entity.attrs.get(
        'kubernetes:load-balancer-ingress').value == '146.145.27.27'
    assert entity.attrs.get(
        'kubernetes:load-balancer-ingress').traits == {'ipaddr:v4'}
    assert len(list(entity.children)) == 2
    assert len(list(entity.parents)) == 1
    assert cobe.UEID('ff290adeb112ae377e8fca009ca4fd9f') in entity.parents
    assert cobe.UEID('04b7f2f17b8067afd71fd4c40d930149') in entity.children
    assert cobe.UEID('8281b9ac13e96fc6af3471cad4047813') in entity.children
    with pytest.raises(StopIteration):
        next(entities_ipv4)
    assert service.logged_k8s_unreachable is False


def test_service_entities_ipv6(entities_ipv6):
    # Test with service having IPv6 ingress point.
    entity = next(entities_ipv6)
    assert entity.attrs.get(
        'kubernetes:load-balancer-ingress'
    ).value == '2001:db8:85a3::8a2e:370:7334'
    assert entity.attrs.get(
        'kubernetes:load-balancer-ingress').traits == {'ipaddr:v6'}


def test_service_entities_hostname(entities_hostname):
    # Test with service having hostname ingress point.
    entity = next(entities_hostname)
    assert entity.attrs.get(
        'kubernetes:load-balancer-ingress'
    ).value == 'derreck'
    assert entity.attrs.get(
        'kubernetes:load-balancer-ingress').traits == set()


def test_missing_attributes_handled(service, cluster_ipv4):
    cluster_ipv4.services = [
        kube.ServiceItem(cluster_ipv4, {
            'metadata': {
                'name': 'test_service',
                'namespace': 'test_namespace',
                'resourceVersion': '12903054',
                'creationTimestamp': '2016-10-03T12:49:32Z',
                'selfLink': 'test_link_path',
                'uid': '7b211c2e-9644-11e6-8a78-42010af00021',
                'labels': {'label1': 'string1',
                           'label2': 'string2'}},
            'status': {}})]
    service.cluster = cluster_ipv4
    entities = service.entityd_find_entity(
        name='Kubernetes:Service', attrs=None, include_ondemand=False)
    entity = next(entities)
    assert entity.metype == 'Kubernetes:Service'
    assert entity.attrs._deleted_attrs == {'kubernetes:load-balancer-ingress'}


def test_k8s_unreachable(service, monkeypatch):
    monkeypatch.setattr(service, 'create_entity',
                        pytest.Mock(side_effect=requests.ConnectionError))
    assert service.logged_k8s_unreachable is False
    assert list(service.entityd_find_entity(
        name='Kubernetes:Service', attrs=None, include_ondemand=False)) == []
    assert service.logged_k8s_unreachable is True


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
