import pytest

import entityd.health


def test_heartbeat():
    entityd.health.heartbeat()
    with pytest.raises(SystemExit) as exception:
        entityd.health.check()
    assert exception.value.code == 0


def test_die():
    entityd.health.die()
    with pytest.raises(SystemExit) as exception:
        entityd.health.check()
    assert exception.value.code == 1


def test_no_heartbeat():
    with pytest.raises(SystemExit) as exception:
        entityd.health.check()
    assert exception.value.code == 1


def test_heartbeat_then_die():
    entityd.health.heartbeat()
    with pytest.raises(SystemExit) as exception_1:
        entityd.health.check()
    entityd.health.die()
    with pytest.raises(SystemExit) as exception_2:
        entityd.health.check()
    assert exception_1.value.code == 0
    assert exception_2.value.code == 1


def test_heartbeat_then_check():
    entityd.health.heartbeat()
    with pytest.raises(SystemExit) as exception_1:
        entityd.health.check()
    with pytest.raises(SystemExit) as exception_2:
        entityd.health.check()
    assert exception_1.value.code == 0
    assert exception_2.value.code == 1
