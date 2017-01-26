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
    """Mock of ``kube.Cluster``.

    This cluster has a Replication Controller having 2 child pods.
    """
    replicationcontrollers = [
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
            'status': {
                'replicas': 1,
                'observedGeneration': 2,
                'fullyLabeledReplicas': 3,
                'readyReplicas': 5,
                'availableReplicas': 6,
            },
            'spec': {
                'replicas': 4
            }})
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
                'replicas': 4}}),
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
    kind = 'Kind.ReplicationController'
    return types.SimpleNamespace(
        replicationcontrollers=replicationcontrollers, pods=pods, kind=kind)


@pytest.fixture
def replicationcontroller(cluster_entity_plugin, pm, config, session):    # pylint: disable=unused-argument
    """Instance of ``replicationcontroller.ReplicationControllerEntity``."""
    replicationcontroller = \
        entityd.kubernetes.replicationcontroller.ReplicationControllerEntity()
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
        name='Kubernetes:'
             'ReplicationController', attrs=None, include_ondemand=False)
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


def test_replicationcontroller_entities(replicationcontroller, entities):
    entity = next(entities)
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
    assert cobe.UEID('5a3a32ba5409a19c744633eb9f81a222') in entity.children
    assert cobe.UEID('43043ccc3b2dc9f57abc64b052e6aa3e') in entity.children
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
        name='Kubernetes:'
             'ReplicationController', attrs=None, include_ondemand=False)
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


def test_no_cluster_ueid_found(session):
    rc_entity = \
        entityd.kubernetes.replicationcontroller.ReplicationControllerEntity()
    rc_entity.entityd_sessionstart(session)
    def entityd_find_entity(name, attrs):    # pylint: disable=unused-argument
        return [[]]
    hooks = types.SimpleNamespace(entityd_find_entity=entityd_find_entity)
    pluginmanager = types.SimpleNamespace(hooks=hooks)
    rc_entity._session = types.SimpleNamespace(
        pluginmanager=pluginmanager)
    with pytest.raises(LookupError):
        assert rc_entity.cluster_ueid
