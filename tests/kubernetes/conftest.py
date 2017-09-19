import cobe
import pytest

from entityd.kubernetes import NamespaceGroup


@pytest.fixture
def namespace_group_ueid(monkeypatch):
    mock = pytest.MagicMock()
    mock.return_value = cobe.UEID("a" * 32)
    monkeypatch.setattr(NamespaceGroup, "get_ueid", mock)
    ueid = NamespaceGroup.get_ueid(None, None)
    return ueid


@pytest.fixture
def cluster_ueid(monkeypatch):
    monkeypatch.setattr(NamespaceGroup, "_cluster_ueid", cobe.UEID("a" * 32))