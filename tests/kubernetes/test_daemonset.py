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
    """Mock of ``kube.Cluster`` with Daemon Set having 2 pods."""
    daemonsets = [
        kube.DaemonSetItem(None, {
            'metadata': {
                'name': 'test_daemonset',
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
                'currentNumberScheduled': 2,
                'numberMisscheduled': 0,
                'desiredNumberScheduled': 3,
                'numberReady': 1,
            },
            'spec': {
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
    kind = 'Kind.DaemonSet'
    proxy = types.SimpleNamespace(get=pytest.Mock(return_value=podsitems))
    pods = types.SimpleNamespace(api_path='apis/extensions/v1beta1')
    return types.SimpleNamespace(daemonsets=daemonsets,
                                 pods=pods, kind=kind, proxy=proxy)


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


def test_daemonset_entities(daemonset, entities, cluster):
    entity = next(entities)
    assert cluster.proxy.get.call_args_list[0][1]['labelSelector'] in [
        'pod-template-hash=1268107570,label1=string1,tier in (cache),'
        'environment notin (dev)',
        'label1=string1,'
        'pod-template-hash=1268107570,tier in (cache),environment notin (dev)',
    ]
    assert cluster.proxy.get.call_args_list[0][0] == (
        'apis/extensions/v1beta1/namespaces/test_namespace/pods',)
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
    assert cobe.UEID('04b7f2f17b8067afd71fd4c40d930149') in entity.children
    assert cobe.UEID('8281b9ac13e96fc6af3471cad4047813') in entity.children
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
