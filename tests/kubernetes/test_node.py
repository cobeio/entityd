import types

import cobe
import pytest
import requests

import kubernetes.node


@pytest.fixture
def cluster():
    """Mock ``kube.Cluster`` of 2 nodes, one having 2 pods, the other 1."""
    class Cluster:
        def __enter__(self):
            nodes = [
                types.SimpleNamespace(
                    meta=types.SimpleNamespace(name='nodename1'),
                    raw={
                        'status': {
                            'nodeInfo': {
                                'bootID':
                                    'd4e0c0ae-290c-4e79-ae78-88b5d6cf215b',
                        }}}),
                types.SimpleNamespace(
                    meta=types.SimpleNamespace(name='nodename2'),
                    raw={
                        'status': {
                            'nodeInfo': {
                                'bootID':
                                    'f5c1d4bf-173f-5c51-bf32-97c4e2eg123e',
                        }}})]
            pods = [
                types.SimpleNamespace(
                    raw={
                        'metadata': {
                            'name': 'podname1-v3-ut4bz',
                            'namespace': 'namespace1',
                        },
                        'spec': {
                            'nodeName': 'nodename1'}}),
                types.SimpleNamespace(
                    raw={
                        'metadata': {
                            'name': 'podname2-v3-xa5at',
                            'namespace': 'namespace2',
                        },
                        'spec': {
                            'nodeName': 'nodename1'}}),
                types.SimpleNamespace(
                    raw={
                        'metadata': {
                            'name': 'podname3-v1-ab2yx',
                            'namespace': 'namespace3',
                        },
                        'spec': {
                            'nodeName': 'nodename2'}})]
            cluster = types.SimpleNamespace(nodes=nodes, pods=pods)
            return cluster
        def __exit__(self, exc, val, tb):
            pass
    return Cluster


@pytest.fixture
def cluster_missing_node():
    """"Mock ``kube.Cluster`` that includes a pod with no node info.

    In practice, this might be due to the pod not yet having been fully
    established within k8s; e.g. awaiting disks.
    """
    class Cluster:
        def __enter__(self):
            nodes = [
                types.SimpleNamespace(
                    meta=types.SimpleNamespace(name='nodename1'),
                    raw={
                        'status': {
                            'nodeInfo': {
                                'bootID':
                                    'd4e0c0ae-290c-4e79-ae78-88b5d6cf215b',
                        }}})]
            pods = [
                types.SimpleNamespace(
                    raw={
                        'metadata': {
                            'name': 'podname1-v3-ut4bz',
                            'namespace': 'namespace1',
                        },
                        'spec': {
                            'nodeName': 'nodename1'}}),
                types.SimpleNamespace(
                    raw={
                        'metadata': {
                            'name': 'podname3-v1-ab2yx',
                            'namespace': 'namespace3',
                        },
                        'spec': {},
                    })]
            cluster = types.SimpleNamespace(nodes=nodes, pods=pods)
            return cluster
        def __exit__(self, exc, val, tb):
            pass
    return Cluster


@pytest.fixture
def cluster_unreachable():
    """Mock ``kube.Cluster`` where kubernetes is unreachable."""
    class Cluster:
        def __enter__(self):
            nodes = pytest.Mock(side_effect=requests.ConnectionError)
            pods = pytest.Mock(side_effect=requests.ConnectionError)
            cluster = types.SimpleNamespace(nodes=nodes, pods=pods)
            return cluster
        def __exit__(self, exc, val, tb):
            pass
    return Cluster


@pytest.fixture
def node(pm, config, session):
    """Fixture providing instance of ``node.NodeEntity``."""
    node = kubernetes.node.NodeEntity()
    pm.register(node, name='kubernetes.node.NodeEntity')
    node.entityd_sessionstart(session)
    node.entityd_configure(config)
    return node


@pytest.fixture
def entities(node, monkeypatch, cluster):
    """Fixture providing entities."""
    entities = node.entityd_find_entity(
        name='Kubernetes:Node', attrs=None, include_ondemand=False)
    monkeypatch.setattr(kubernetes.node.kube, 'Cluster', cluster)
    return entities


@pytest.fixture
def entities_missing_nodename(node, monkeypatch,
                              cluster_missing_node):
    """Fixture providing entities where pod doesn't have nodename assigned."""
    entities = node.entityd_find_entity(
        name='Kubernetes:Node', attrs=None, include_ondemand=False)
    monkeypatch.setattr(kubernetes.node.kube, 'Cluster', cluster_missing_node)
    return entities


def test_find_entity_with_attrs_not_none(node):
    with pytest.raises(LookupError):
        node.entityd_find_entity(
            'Kubernetes:Node', {'attr': 'foo-entity-bar'})


def test_get_first_entity(entities):
    entity = next(entities)
    assert entity.metype == 'Kubernetes:Node'
    assert entity.label == 'nodename1'
    assert entity.attrs.get(
        'bootid').value == 'd4e0c0ae-290c-4e79-ae78-88b5d6cf215b'
    assert entity.attrs.get('bootid').traits == {'entity:id'}
    assert entity.attrs.get('kubernetes:kind').value == 'Node'
    assert len(list(entity.children)) == 2
    assert cobe.UEID('0865915cc5328bea2434c86599693e0d') in entity.children
    assert cobe.UEID('0dc792da162422b677f47cfa551e50be') in entity.children
    assert kubernetes.node._LOGGED_K8S_UNREACHABLE == False


def test_get_second_entity(entities):
    _ = next(entities)
    entity = next(entities)
    assert entity.metype == 'Kubernetes:Node'
    assert entity.label == 'nodename2'
    assert entity.attrs.get(
        'bootid').value == 'f5c1d4bf-173f-5c51-bf32-97c4e2eg123e'
    assert entity.attrs.get('bootid').traits == {'entity:id'}
    assert entity.attrs.get('kubernetes:kind').value == 'Node'
    assert len(list(entity.children)) == 1
    assert cobe.UEID('7d37a350012e63f5713a66497f609a92') in entity.children


def test_get_entities_with_pod_missing_nodename(entities_missing_nodename):
    entity = next(entities_missing_nodename)
    assert entity.label == 'nodename1'
    assert len(list(entity.children)) == 1
    assert cobe.UEID('0865915cc5328bea2434c86599693e0d') in entity.children
    with pytest.raises(StopIteration):
        next(entities_missing_nodename)


def test_k8s_unreachable(node, monkeypatch, pm, config,
                         session, cluster_unreachable):
    monkeypatch.setattr(kubernetes.node.NodeEntity, 'getnodespods',
                        pytest.Mock(side_effect=requests.ConnectionError))
    assert kubernetes.node._LOGGED_K8S_UNREACHABLE == False
    assert list(node.entityd_find_entity(
        name='Kubernetes:Node', attrs=None, include_ondemand=False)) == []
    assert kubernetes.node._LOGGED_K8S_UNREACHABLE == True
