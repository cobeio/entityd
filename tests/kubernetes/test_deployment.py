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
    """Mock of ``kube.Cluster`` with a deployment."""
    deployments = [
        kube.DeploymentItem(None, {
            'metadata': {
                'name': 'test_deployment',
                'namespace': 'test_namespace',
                'resourceVersion': '12903054',
                'creationTimestamp': '2016-10-03T12:49:32Z',
                'selfLink': 'test_link_path',
                'uid': '7b211c2e-9644-11e6-8a78-42010af00021',
                'labels': {
                    'label2': 'string2',
                    'label3': 'string2',
                },
            },
            'status': {
                'observedGeneration': 1,
                'replicas': 2,
                'updatedReplicas': 3,
                'availableReplicas': 5,
                'unavailableReplicas': 6,
            },
            'spec': {
                'replicas': 4,
                'selector': {
                    'matchLabels': {
                        'pod-template-hash': '1268107570',
                        'label1': 'string1',
                    },
                    'matchExpressions': [
                        {
                            'key': 'tier',
                            'operator': 'In',
                            'values': ['cache'],
                        },
                        {
                            'key': 'environment',
                            'operator': 'NotIn',
                            'values': ['dev'],
                        },
                    ],
                },
            },
        }),
    ]
    rsitems = {
        'items': [
            {
                'metadata': {
                    'name': 'rsname1-1268107570',
                    'namespace': 'test_namespace',
                },
            },
        ],
    }
    kind = 'Kind.Deployment'
    proxy = types.SimpleNamespace(get=pytest.Mock(return_value=rsitems))
    replicasets = types.SimpleNamespace(api_path='apis/extensions/v1beta1')
    return types.SimpleNamespace(deployments=deployments,
                                 replicasets=replicasets,
                                 kind=kind, proxy=proxy)


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


def test_deployment_entities(deployment, entities, cluster):
    entity = next(entities)
    assert cluster.proxy.get.call_args_list[0][1]['labelSelector'] in [
        'pod-template-hash=1268107570,label1=string1,tier in (cache),'
        'environment notin (dev)',
        'label1=string1,'
        'pod-template-hash=1268107570,tier in (cache),environment notin (dev)',
    ]
    assert cluster.proxy.get.call_args_list[0][0] == (
        'apis/extensions/v1beta1/namespaces/test_namespace/replicasets',)
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
    assert entity.attrs.get('kubernetes:observed-replicas').value == 2
    assert entity.attrs.get('kubernetes:updated-replicas').value == 3
    assert entity.attrs.get('kubernetes:available-replicas').value == 5
    assert entity.attrs.get('kubernetes:unavailable-replicas').value == 6
    assert entity.attrs.get('kubernetes:replicas-desired').value == 4
    assert len(list(entity.children)) == 1
    assert len(list(entity.parents)) == 1
    assert cobe.UEID('ff290adeb112ae377e8fca009ca4fd9f') in entity.parents
    assert cobe.UEID('7535ca32b5337aeaf4e7d14b15a0b052') in entity.children
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
