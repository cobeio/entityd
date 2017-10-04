import cobe
import kube
import pytest
import requests

from entityd.kubernetes.group import NamespaceGroup


@pytest.fixture
def active_namespace():
    ns = kube.NamespaceItem(None, {
        'metadata': {
            'name': 'namespace-1',
        },
        'status': {
            'phase': 'Active',
        },
    })
    return ns


@pytest.fixture
def terminating_namespace():
    ns = kube.NamespaceItem(None, {
        'metadata': {
            'name': 'namespace-2',
        },
        'status': {
            'phase': 'Terminating',
        },
    })
    return ns


@pytest.fixture
def namespace_group(pm):    # pylint: disable=unused-argument
    """Fixture providing instance of ``group.NamespaceGroup``."""
    group = NamespaceGroup()
    pm.register(
        group, name='entityd.kubernetes.group.NamespaceGroup')

    return group


@pytest.fixture
def cluster(session, active_namespace, namespace_group, terminating_namespace):
    namespaces = [active_namespace, terminating_namespace]

    cluster = pytest.MagicMock()
    cluster.namespaces = namespaces

    session.addservice("kube_cluster", cluster)
    namespace_group.entityd_sessionstart(session)
    namespace_group.entityd_configure(session.config)
    return cluster


def test_attrs_raises_exception():
    group = NamespaceGroup()
    with pytest.raises(LookupError):
        group.entityd_find_entity(
            NamespaceGroup.name,
            attrs="foo")


def test_not_provided():
    group = NamespaceGroup()
    assert group.entityd_find_entity('foo') is None


def test_find_entity_with_attrs_not_none(namespace_group):
    with pytest.raises(LookupError):
        namespace_group.entityd_find_entity(
            'Group', {'attr': 'foo-entity-bar'})


def test_cluster_ueid_not_found(session):
    NamespaceGroup._cluster_ueid = None
    session.pluginmanager.hooks.entityd_find_entity.return_value = None

    with pytest.raises(LookupError):
        NamespaceGroup.get_cluster_ueid(session)


def test_k8s_unreachable(cluster, namespace_group):
    class ErrorIter:
        def __iter__(self):
            raise requests.ConnectionError

    cluster.namespaces = ErrorIter()

    assert namespace_group.logged_k8s_unreachable is False
    assert list(namespace_group.entityd_find_entity(
        name='Group',
        attrs=None, include_ondemand=False)) == []
    assert namespace_group.logged_k8s_unreachable is True


def test_find_entities(monkeypatch, cluster, namespace_group,    # pylint: disable=unused-argument
                       session, active_namespace, terminating_namespace):
    monkeypatch.setattr(NamespaceGroup, "_cluster_ueid", cobe.UEID("a" * 32))

    namespaces = [active_namespace, terminating_namespace]

    entities = namespace_group.entityd_find_entity(namespace_group.name)
    entities = list(entities)
    assert len(entities) == 2

    zipped = zip(entities, namespaces)
    for entity, namespace in zipped:
        assert entity.attrs.get('kind').value == "Kubernetes:Namespace"
        namespace_group_ueid = NamespaceGroup.get_ueid(
            namespace.meta.name, session)
        assert namespace_group_ueid  == entity.ueid
        namespace_ueid = NamespaceGroup.create_namespace_ueid(
            namespace.meta.name, session)
        assert namespace_ueid in entity.children
