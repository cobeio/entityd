import kube
import pytest

import entityd.kubernetes
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


@pytest.mark.parametrize(
    ('input_', 'output'),
    [
        ('1000m', 100),
        ('250m', 25),
        ('1', 100),
        ('0.1', 10),
    ]
)
def test_cpu_conversion(input_, output):
    assert entityd.kubernetes.cpu_conversion(input_) == output


@pytest.mark.parametrize('input', ['test'])
def test_cpu_conversion_exception(input):
    with pytest.raises(ValueError):
        entityd.kubernetes.cpu_conversion(input)


@pytest.mark.parametrize(
    ('input_', 'output'),
    [
        ('inf', float('inf')),
        ('129e6', 129000000),
        ('128974848', 128974848),
        ('10Ki', 10240),
        ('10Mi', 10485760),
        ('10Gi', 10737418240),
        ('10Ti', 10995116277760),
        ('10Pi', 11258999068426240),
        ('10Ei', 11529215046068469760),
        ('10K', 10000),
        ('10M', 10000000),
        ('10G', 10000000000),
        ('10T', 10000000000000),
        ('10P', 10000000000000000),
        ('10E', 10000000000000000000),
    ],
)
def test_ram_conversion(input_, output):
    assert entityd.kubernetes.ram_conversion(input_) == output


@pytest.mark.parametrize('input', ['test', 1, '10X', 'K', 'e'])
def test_ram_conversion_exception(input):
    with pytest.raises(ValueError):
        entityd.kubernetes.ram_conversion(input)
