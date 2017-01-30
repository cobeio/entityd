import kube
import pytest

import entityd.kubernetes.replicaset


@pytest.fixture
def rsresource():
    """``kube.ReplicaSetItem`` without matchExpressions."""
    rsitem = kube.ReplicaSetItem(None, {
        'metadata': {
            'name': 'test_replicaset',
            'namespace': 'test_namespace',
        },
        'spec': {
            'replicas': 4,
            'selector': {
                'matchLabels': {
                    'pod-template-hash': '1268107570',
                    'label1': 'string1',
                },
            }
        }
    })
    return rsitem


@pytest.fixture
def rs_no_labels():
    """``kube.ReplicaSetItem`` without any selector labels."""
    rsitem = kube.ReplicaSetItem(None, {
        'metadata': {
            'name': 'test_replicaset',
            'namespace': 'test_namespace',
        },
        'spec': {
            'replicas': 4,
            'selector': {},
        }
    })
    return rsitem


def test_resource_pod_children(rsresource, monkeypatch):
    rset = entityd.kubernetes.replicaset.ReplicaSetEntity()
    findmock = pytest.Mock()
    monkeypatch.setattr(rset, 'find_pods_by_labels', findmock)
    rset.find_resource_pod_children(rsresource, 'api/v1')
    assert findmock.call_args_list[0][0][1] in [
        'pod-template-hash=1268107570,label1=string1',
        'label1=string1,pod-template-hash=1268107570',
    ]
    assert findmock.call_args_list[0][0][2] == 'api/v1'


def test_resource_pod_children_no_sel_labels(rs_no_labels,
                                             monkeypatch):
    rset = entityd.kubernetes.replicaset.ReplicaSetEntity()
    findmock = pytest.Mock()
    monkeypatch.setattr(rset, 'find_pods_by_labels', findmock)
    rset.find_resource_pod_children(rs_no_labels, 'api/v1')
    assert findmock.call_args_list[0][0][1] in ['']
    assert findmock.call_args_list[0][0][2] == 'api/v1'
