import types

import cobe
import kube
import pytest
import requests

import entityd.kubernetes
import entityd.kubernetes.cluster
import entityd.kubernetes.replicationcontroller
import entityd.pm


@pytest.fixture
def cluster():
    """Mock of ``kube.Cluster`` with Replication Controller having 2 pods."""
    replicationcontrollers = [
        kube.ReplicationControllerItem(None, {
            'metadata': {
                'name': 'test_replicationcontroller',
                'namespace': 'test_namespace',
                'resourceVersion': '12903054',
                'creationTimestamp': '2016-10-03T12:49:32Z',
                'selfLink': 'test_link_path',
                'uid': '7b211c2e-9644-11e6-8a78-42010af00021',
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
                    'label1': 'string1',
                    'label2': 'string2',
                },
            },
        }),
    ]
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
    kind = 'Kind.ReplicationController'
    proxy = types.SimpleNamespace(get=pytest.Mock(return_value=podsitems))
    pods = types.SimpleNamespace(api_path='api/v1')
    return types.SimpleNamespace(replicationcontrollers=replicationcontrollers,
                                 pods=pods, kind=kind, proxy=proxy)


@pytest.fixture
def replicationcontroller(cluster_entity_plugin, pm, config, session):    # pylint: disable=unused-argument
    """``replicationcontroller.ReplicationControllerEntity`` instance."""
    replicationcontroller = entityd.kubernetes.\
        replicationcontroller.ReplicationControllerEntity()
    pm.register(
        replicationcontroller,
        name='entityd.kubernetes.'
             'replicationcontroller.ReplicationControllerEntity')
    replicationcontroller.entityd_sessionstart(session)
    replicationcontroller.entityd_configure(config)
    return replicationcontroller


@pytest.fixture
def entities(replicationcontroller, cluster):
    """Fixture providing entities."""
    replicationcontroller.cluster = cluster
    entities = replicationcontroller.entityd_find_entity(
        name='Kubernetes:ReplicationController',
        attrs=None, include_ondemand=False)
    return entities


def test_sessionfinish(replicationcontroller):
    assert isinstance(replicationcontroller.cluster, kube._cluster.Cluster)
    mock = pytest.Mock()
    replicationcontroller.cluster = mock
    replicationcontroller.entityd_sessionfinish()
    mock.close.assert_called_once_with()


def test_find_entity_with_attrs_not_none(replicationcontroller):
    with pytest.raises(LookupError):
        replicationcontroller.entityd_find_entity(
            'Kubernetes:ReplicationController', {'attr': 'foo-entity-bar'})


def test_replicationcontroller_entities(
        replicationcontroller, entities, cluster):
    entity = next(entities)
    assert cluster.proxy.get.call_args_list[0][1]['labelSelector'] in [
        'label1=string1,label2=string2',
        'label2=string2,label1=string1',
    ]
    assert cluster.proxy.get.call_args_list[0][0] == (
        'api/v1/namespaces/test_namespace/pods',)
    assert entity.metype == 'Kubernetes:ReplicationController'
    assert entity.label == 'test_replicationcontroller'
    assert entity.attrs.get('kubernetes:kind').value == 'ReplicationController'
    assert entity.attrs.get(
        'kubernetes:meta:name').value == 'test_replicationcontroller'
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
    assert replicationcontroller.logged_k8s_unreachable is False


def test_missing_attributes_handled(replicationcontroller, cluster):
    cluster.replicationcontrollers = [
        kube.ReplicationControllerItem(cluster, {
            'metadata': {
                'name': 'test_replicationcontroller',
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
    replicationcontroller.cluster = cluster
    entities = replicationcontroller.entityd_find_entity(
        name='Kubernetes:ReplicationController',
        attrs=None,
        include_ondemand=False)
    entity = next(entities)
    assert entity.metype == 'Kubernetes:ReplicationController'
    assert entity.attrs._deleted_attrs == {
        'kubernetes:observed-replicas',
        'kubernetes:observed-generation',
        'kubernetes:fully-labeled-replicas',
        'kubernetes:ready-replicas',
        'kubernetes:available-replicas',
        'kubernetes:replicas-desired',
    }


def test_k8s_unreachable(replicationcontroller, monkeypatch):
    monkeypatch.setattr(replicationcontroller, 'create_entity',
                        pytest.Mock(side_effect=requests.ConnectionError))
    assert replicationcontroller.logged_k8s_unreachable is False
    assert list(replicationcontroller.entityd_find_entity(
        name='Kubernetes:ReplicationController',
        attrs=None, include_ondemand=False)) == []
    assert replicationcontroller.logged_k8s_unreachable is True
