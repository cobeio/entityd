import types

import cobe
import kube
import pytest
import requests

import entityd.kubernetes
import entityd.kubernetes.cluster
import entityd.kubernetes.replicaset
import entityd.pm


@pytest.fixture
def cluster():
    """Mock of ``kube.Cluster`` with Replica Set having 2 pods."""
    replicasets = [
        kube.ReplicaSetItem(None, {
            'metadata': {
                'name': 'test_replicaset',
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
                'replicas': 1,
                'observedGeneration': 2,
                'fullyLabeledReplicas': 3,
                'readyReplicas': 5,
                'availableReplicas': 6,
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
    deployments = []
    podsitems = {
        'items': [
            {
                'metadata': {
                    'name': 'podname1-v3-ut4bz',
                    'namespace': 'test_namespace',
                },
            },
            {
                'metadata': {
                    'name': 'podname2-v3-xa5at',
                    'namespace': 'test_namespace',
                },
            },
        ],
    }
    kind = 'Kind.ReplicaSet'
    proxy = types.SimpleNamespace(get=pytest.Mock(return_value=podsitems))
    pods = types.SimpleNamespace(api_path='apis/extensions/v1beta1')

    class TestReplicaSetView(kube.ReplicaSetView):

        def __iter__(self):
            yield from replicasets

        @property
        def api_path(self):
            return 'apis/extensions/v1beta1'

    return types.SimpleNamespace(
        replicasets=TestReplicaSetView(pytest.Mock()),
        deployments=deployments,
        pods=pods,
        kind=kind,
        proxy=proxy,
    )


@pytest.fixture
def deployment(cluster):
    deployment = kube.DeploymentItem(
        None,
        {
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
                },
            },
        },
    )
    cluster.deployments.append(deployment)
    return deployment


@pytest.fixture
def replicaset(cluster_entity_plugin, pm, config, session):    # pylint: disable=unused-argument
    """Fixture providing instance of ``replicaset.ReplicaSetEntity``."""
    replicaset = entityd.kubernetes.replicaset.ReplicaSetEntity()
    pm.register(
        replicaset, name='entityd.kubernetes.replicaset.ReplicaSetEntity')
    replicaset.entityd_sessionstart(session)
    replicaset.entityd_configure(config)
    return replicaset


@pytest.fixture
def entities(replicaset, cluster):
    """Fixture providing entities."""
    replicaset.cluster = cluster
    entities = replicaset.entityd_find_entity(
        name='Kubernetes:ReplicaSet', attrs=None, include_ondemand=False)
    return entities


def test_sessionfinish(replicaset):
    assert isinstance(replicaset.cluster, kube._cluster.Cluster)
    mock = pytest.Mock()
    replicaset.cluster = mock
    replicaset.entityd_sessionfinish()
    mock.close.assert_called_once_with()


def test_find_entity_with_attrs_not_none(replicaset):
    with pytest.raises(LookupError):
        replicaset.entityd_find_entity(
            'Kubernetes:ReplicaSet', {'attr': 'foo-entity-bar'})


def test_replicaset_entities(replicaset, entities, cluster):
    entity = next(entities)
    assert cluster.proxy.get.call_args_list[0][1]['labelSelector'] in [
        'pod-template-hash=1268107570,label1=string1,tier in (cache),'
        'environment notin (dev)',
        'label1=string1,'
        'pod-template-hash=1268107570,tier in (cache),environment notin (dev)',
    ]
    assert cluster.proxy.get.call_args_list[0][0] == (
        'apis/extensions/v1beta1/namespaces/test_namespace/pods',)
    assert entity.metype == 'Kubernetes:ReplicaSet'
    assert entity.label == 'test_replicaset'
    assert entity.attrs.get('kubernetes:kind').value == 'ReplicaSet'
    assert entity.attrs.get('kubernetes:meta:name').value == 'test_replicaset'
    assert entity.attrs.get('kubernetes:meta:version').value == '12903054'
    assert entity.attrs.get(
        'kubernetes:meta:created').value == '2016-10-03T12:49:32Z'
    assert entity.attrs.get('kubernetes:meta:link').value == 'test_link_path'
    assert entity.attrs.get('kubernetes:meta:link').traits == {'uri'}
    assert entity.attrs.get(
        'kubernetes:meta:uid').value == '7b211c2e-9644-11e6-8a78-42010af00021'
    assert entity.attrs.get('kubernetes:observed-replicas').value == 1
    assert entity.attrs.get('kubernetes:observed-generation').value == 2
    assert entity.attrs.get('kubernetes:fully-labeled-replicas').value == 3
    assert entity.attrs.get('kubernetes:ready-replicas').value == 5
    assert entity.attrs.get('kubernetes:available-replicas').value == 6
    assert entity.attrs.get('kubernetes:replicas-desired').value == 4
    assert len(list(entity.children)) == 2
    assert len(list(entity.parents)) == 1
    assert cobe.UEID('ff290adeb112ae377e8fca009ca4fd9f') in entity.parents
    assert cobe.UEID('04b7f2f17b8067afd71fd4c40d930149') in entity.children
    assert cobe.UEID('8281b9ac13e96fc6af3471cad4047813') in entity.children
    with pytest.raises(StopIteration):
        next(entities)
    assert replicaset.logged_k8s_unreachable is False


def test_replicaset_has_deployment(
        monkeypatch, replicaset, deployment, cluster):  # pylint: disable=unused-argument
    replicaset.cluster = cluster
    ueid = next(replicaset.entityd_find_entity(
        name='Kubernetes:ReplicaSet',
        attrs=None,
        include_ondemand=False,
    )).ueid
    monkeypatch.setattr(
        replicaset,
        'find_deployment_rs_children',
        pytest.Mock(return_value=[ueid]),
    )
    entity = next(replicaset.entityd_find_entity(
        name='Kubernetes:ReplicaSet',
        attrs=None,
        include_ondemand=False,
    ))
    assert len(entity.parents) == 0


def test_missing_attributes_handled(replicaset, cluster):
    cluster.replicasets = [
        kube.ReplicaSetItem(cluster, {
            'metadata': {
                'name': 'test_replicaset',
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
    replicaset.cluster = cluster
    entities = replicaset.entityd_find_entity(
        name='Kubernetes:ReplicaSet', attrs=None, include_ondemand=False)
    entity = next(entities)
    assert entity.metype == 'Kubernetes:ReplicaSet'
    assert entity.attrs._deleted_attrs == {
        'kubernetes:observed-replicas',
        'kubernetes:observed-generation',
        'kubernetes:fully-labeled-replicas',
        'kubernetes:ready-replicas',
        'kubernetes:available-replicas',
        'kubernetes:replicas-desired',
    }


def test_k8s_unreachable(replicaset, monkeypatch):
    monkeypatch.setattr(replicaset, 'create_entity',
                        pytest.Mock(side_effect=requests.ConnectionError))
    assert replicaset.logged_k8s_unreachable is False
    assert list(replicaset.entityd_find_entity(
        name='Kubernetes:ReplicaSet',
        attrs=None, include_ondemand=False)) == []
    assert replicaset.logged_k8s_unreachable is True


def test_no_cluster_ueid_found(session):
    replicasetentity = entityd.kubernetes.replicaset.ReplicaSetEntity()
    replicasetentity.entityd_sessionstart(session)
    def entityd_find_entity(name, attrs):    # pylint: disable=unused-argument
        return [[]]
    hooks = types.SimpleNamespace(entityd_find_entity=entityd_find_entity)
    pluginmanager = types.SimpleNamespace(hooks=hooks)
    replicasetentity._session = types.SimpleNamespace(
        pluginmanager=pluginmanager)
    with pytest.raises(LookupError):
        assert replicasetentity.cluster_ueid
