import types

import cobe
import kube
import pytest
import requests

import entityd.kubernetes
import entityd.kubernetes.cluster
import entityd.kubernetes.deployment
import entityd.pm


@pytest.fixture
def cluster():
    """Mock of ``kube.Cluster`` with Deployment having 2 child replicasets."""
    deployments = [
        kube.DeploymentItem(cluster, {
            'metadata': {
                'name': 'test_deployment',
                'namespace': 'test_namespace',
                'resourceVersion': '12903054',
                'creationTimestamp': '2016-10-03T12:49:32Z',
                'selfLink': 'test_link_path',
                'uid': '7b211c2e-9644-11e6-8a78-42010af00021',
                'labels': {'label1': 'string1',
                           'label2': 'string2'}
            },
            'status': {
                'observedGeneration': 1,
                'replicas': 3,
                'updatedReplicas': 2,
                'availableReplicas': 1,
                'unavailableReplicas': 2,
            },
            'spec': {
                'replicas': 3
            }})
    ]
    replicasets = [
        kube.ReplicaSetItem(cluster, {
            'metadata': {
                'name': 'rs-name1-v3-ut4bz',
                'namespace': 'namespace',
                'labels': {'label1': 'string1',
                           'label2': 'string2',
                           'label3': 'string3'},
            }}),
        kube.ReplicaSetItem(cluster, {
            'metadata': {
                'name': 'rs-name2-v3-xa5at',
                'namespace': 'namespace',
                'labels': {'label1': 'string1',
                           'label2': 'string2',
                           'label4': 'string4'},
            }}),
        kube.ReplicaSetItem(cluster, {
            'metadata': {
                'name': 'rs-name3-v3-tv9zw',
                'namespace': 'namespace',
                'labels': {'label5': 'string5',
                           'label6': 'string6'},
            }}),
    ]
    kind = 'Kind.Deployment'
    return types.SimpleNamespace(deployments=deployments,
                                 replicasets=replicasets,
                                 kind=kind)


@pytest.fixture
def deployment(cluster_entity_plugin, pm, config, session):    # pylint: disable=unused-argument
    """Fixture providing instance of ``deployment.DeploymentEntity``."""
    deployment = entityd.kubernetes.deployment.DeploymentEntity()
    pm.register(
        deployment, name='entityd.kubernetes.deployment.DeploymentEntity')
    deployment.entityd_sessionstart(session)
    deployment.entityd_configure(config)
    return deployment


@pytest.fixture
def entities(deployment, cluster):
    """Fixture providing entities."""
    deployment.cluster = cluster
    entities = deployment.entityd_find_entity(
        name='Kubernetes:Deployment', attrs=None, include_ondemand=False)
    return entities


def test_sessionfinish(deployment):
    assert isinstance(deployment.cluster, kube._cluster.Cluster)
    mock = pytest.Mock()
    deployment.cluster = mock
    deployment.entityd_sessionfinish()
    mock.close.assert_called_once_with()


def test_find_entity_with_attrs_not_none(deployment):
    with pytest.raises(LookupError):
        deployment.entityd_find_entity(
            'Kubernetes:Deployment', {'attr': 'foo-entity-bar'})


def test_deployment_entities(deployment, entities):
    entity = next(entities)
    assert entity.metype == 'Kubernetes:Deployment'
    assert entity.label == 'test_deployment'
    assert entity.attrs.get('kubernetes:kind').value == 'Deployment'
    assert entity.attrs.get('kubernetes:meta:name').value == 'test_deployment'
    assert entity.attrs.get('kubernetes:meta:version').value == '12903054'
    assert entity.attrs.get(
        'kubernetes:meta:created').value == '2016-10-03T12:49:32Z'
    assert entity.attrs.get('kubernetes:meta:link').value == 'test_link_path'
    assert entity.attrs.get('kubernetes:meta:link').traits == {'uri'}
    assert entity.attrs.get(
        'kubernetes:meta:uid').value == '7b211c2e-9644-11e6-8a78-42010af00021'
    assert entity.attrs.get('kubernetes:observed-generation').value == 1
    assert entity.attrs.get('kubernetes:observed-replicas').value == 3
    assert entity.attrs.get('kubernetes:updated-replicas').value == 2
    assert entity.attrs.get('kubernetes:available-replicas').value == 1
    assert entity.attrs.get('kubernetes:unavailable-replicas').value == 2
    assert entity.attrs.get('kubernetes:replicas-desired').value == 3
    assert len(list(entity.children)) == 2
    assert len(list(entity.parents)) == 1
    assert cobe.UEID('ff290adeb112ae377e8fca009ca4fd9f') in entity.parents
    assert cobe.UEID('72ce22397a8603236d4cb0d163e8f5b0') in entity.children
    assert cobe.UEID('ef0084bcb528c95a942d11860c8f0960') in entity.children
    assert cobe.UEID('114d33b9870a80ad4c3a69b5f10ec30a') not in entity.children
    with pytest.raises(StopIteration):
        next(entities)
    assert deployment.logged_k8s_unreachable is False


def test_missing_attributes_handled(deployment, cluster):
    cluster.deployments = [
        kube.DeploymentItem(cluster, {
            'metadata': {
                'name': 'test_deployment',
                'namespace': 'test_namespace',
                'resourceVersion': '12903054',
                'creationTimestamp': '2016-10-03T12:49:32Z',
                'selfLink': 'test_link_path',
                'uid': '7b211c2e-9644-11e6-8a78-42010af00021',
                'labels': {'label1': 'string1',
                           'label2': 'string2'}
            },
            'status': {},
            'spec': {},
        })
    ]
    deployment.cluster = cluster
    entities = deployment.entityd_find_entity(
        name='Kubernetes:Deployment', attrs=None, include_ondemand=False)
    entity = next(entities)
    assert entity.metype == 'Kubernetes:Deployment'
    assert entity.attrs._deleted_attrs == {
        'kubernetes:observed-generation',
        'kubernetes:observed-replicas',
        'kubernetes:updated-replicas',
        'kubernetes:available-replicas',
        'kubernetes:unavailable-replicas',
        'kubernetes:replicas-desired',
    }


def test_k8s_unreachable(deployment, monkeypatch):
    monkeypatch.setattr(deployment, 'create_entity',
                        pytest.Mock(side_effect=requests.ConnectionError))
    assert deployment.logged_k8s_unreachable is False
    assert list(deployment.entityd_find_entity(
        name='Kubernetes:Deployment',
        attrs=None, include_ondemand=False)) == []
    assert deployment.logged_k8s_unreachable is True


def test_no_cluster_ueid_found(session):
    deploymententity = entityd.kubernetes.deployment.DeploymentEntity()
    deploymententity.entityd_sessionstart(session)
    def entityd_find_entity(name, attrs):    # pylint: disable=unused-argument
        return [[]]
    hooks = types.SimpleNamespace(entityd_find_entity=entityd_find_entity)
    pluginmanager = types.SimpleNamespace(hooks=hooks)
    deploymententity._session = types.SimpleNamespace(
        pluginmanager=pluginmanager)
    with pytest.raises(LookupError):
        assert deploymententity.cluster_ueid
