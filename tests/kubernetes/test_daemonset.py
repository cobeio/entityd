import types

import cobe
import kube
import pytest
import requests

import entityd.kubernetes
import entityd.kubernetes.cluster
import entityd.kubernetes.daemonset
import entityd.pm


@pytest.fixture
def cluster():
    """Mock of ``kube.Cluster`` with Daemon Set having 2 child pods."""
    daemonsets = [
        kube.DaemonSetItem(cluster, {
            'metadata': {
                'name': 'test_daemonset',
                'namespace': 'test_namespace',
                'resourceVersion': '12903054',
                'creationTimestamp': '2016-10-03T12:49:32Z',
                'selfLink': 'test_link_path',
                'uid': '7b211c2e-9644-11e6-8a78-42010af00021',
                'labels': {'label1': 'string1',
                           'label2': 'string2'}
            },
            'status': {
                'currentNumberScheduled': 2,
                'numberMisscheduled': 0,
                'desiredNumberScheduled': 3,
                'numberReady': 1,
            }}),
    ]
    pods = [
        kube.PodItem(cluster, {
            'metadata': {
                'name': 'podname1-v3-ut4bz',
                'namespace': 'namespace',
                # todo: should labels be exactly same?
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
    kind = 'Kind.DaemonSet'
    return types.SimpleNamespace(daemonsets=daemonsets, pods=pods, kind=kind)


@pytest.fixture
def daemonset(cluster_entity_plugin, pm, config, session):    # pylint: disable=unused-argument
    """Fixture providing instance of ``daemonset.DaemonSetEntity``."""
    daemonset = entityd.kubernetes.daemonset.DaemonSetEntity()
    pm.register(
        daemonset, name='entityd.kubernetes.daemonset.DaemonSetEntity')
    daemonset.entityd_sessionstart(session)
    daemonset.entityd_configure(config)
    return daemonset


@pytest.fixture
def entities(daemonset, cluster):
    """Fixture providing entities."""
    daemonset.cluster = cluster
    entities = daemonset.entityd_find_entity(
        name='Kubernetes:DaemonSet', attrs=None, include_ondemand=False)
    return entities


def test_sessionfinish(daemonset):
    assert isinstance(daemonset.cluster, kube._cluster.Cluster)
    mock = pytest.Mock()
    daemonset.cluster = mock
    daemonset.entityd_sessionfinish()
    mock.close.assert_called_once_with()


def test_find_entity_with_attrs_not_none(daemonset):
    with pytest.raises(LookupError):
        daemonset.entityd_find_entity(
            'Kubernetes:DaemonSet', {'attr': 'foo-entity-bar'})


def test_daemonset_entities(daemonset, entities):
    entity = next(entities)
    assert entity.metype == 'Kubernetes:DaemonSet'
    assert entity.label == 'test_daemonset'
    assert entity.attrs.get('kubernetes:kind').value == 'DaemonSet'
    assert entity.attrs.get('kubernetes:meta:name').value == 'test_daemonset'
    assert entity.attrs.get('kubernetes:meta:version').value == '12903054'
    assert entity.attrs.get(
        'kubernetes:meta:created').value == '2016-10-03T12:49:32Z'
    assert entity.attrs.get('kubernetes:meta:link').value == 'test_link_path'
    assert entity.attrs.get('kubernetes:meta:link').traits == {'uri'}
    assert entity.attrs.get(
        'kubernetes:meta:uid').value == '7b211c2e-9644-11e6-8a78-42010af00021'
    assert entity.attrs.get('kubernetes:current-number-scheduled').value == 2
    assert entity.attrs.get('kubernetes:number-misscheduled').value == 0
    assert entity.attrs.get('kubernetes:desired-number-scheduled').value == 3
    assert entity.attrs.get('kubernetes:number-ready').value == 1
    assert len(list(entity.children)) == 2
    assert len(list(entity.parents)) == 1
    assert cobe.UEID('ff290adeb112ae377e8fca009ca4fd9f') in entity.parents
    assert cobe.UEID('5a3a32ba5409a19c744633eb9f81a222') in entity.children
    assert cobe.UEID('43043ccc3b2dc9f57abc64b052e6aa3e') in entity.children
    with pytest.raises(StopIteration):
        next(entities)
    assert daemonset.logged_k8s_unreachable is False


def test_missing_attributes_handled(daemonset, cluster):
    cluster.daemonsets = [
        kube.DaemonSetItem(cluster, {
            'metadata': {
                'name': 'test_daemonset',
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
    daemonset.cluster = cluster
    entities = daemonset.entityd_find_entity(
        name='Kubernetes:DaemonSet', attrs=None, include_ondemand=False)
    entity = next(entities)
    assert entity.metype == 'Kubernetes:DaemonSet'
    assert entity.attrs._deleted_attrs == {
        'kubernetes:current-number-scheduled',
        'kubernetes:number-misscheduled',
        'kubernetes:desired-number-scheduled',
        'kubernetes:number-ready',
    }


def test_k8s_unreachable(daemonset, monkeypatch):
    monkeypatch.setattr(daemonset, 'create_entity',
                        pytest.Mock(side_effect=requests.ConnectionError))
    assert daemonset.logged_k8s_unreachable is False
    assert list(daemonset.entityd_find_entity(
        name='Kubernetes:DaemonSet',
        attrs=None, include_ondemand=False)) == []
    assert daemonset.logged_k8s_unreachable is True


def test_no_cluster_ueid_found(session):
    daemonsetentity = entityd.kubernetes.daemonset.DaemonSetEntity()
    daemonsetentity.entityd_sessionstart(session)
    def entityd_find_entity(name, attrs):    # pylint: disable=unused-argument
        return [[]]
    hooks = types.SimpleNamespace(entityd_find_entity=entityd_find_entity)
    pluginmanager = types.SimpleNamespace(hooks=hooks)
    daemonsetentity._session = types.SimpleNamespace(
        pluginmanager=pluginmanager)
    with pytest.raises(LookupError):
        assert daemonsetentity.cluster_ueid
