import pytest

from entityd.docker.client import DockerClient


@pytest.fixture(autouse=True)
def clear_client():
    yield
    DockerClient._client = None


@pytest.fixture
def running_container():
    attrs = {
        "State": {
            "ExitCode": 0,
            "StartedAt": "2017-08-30T10:52:25.439434269Z",
            "Error": "",
            "FinishedAt": "0001-01-01T00:00:00Z"}
    }
    image = pytest.MagicMock(id='image_id', tags=['debian:latest'])
    container = pytest.Mock(
        id="bar", name="running_container", status="running", labels=["label"],
        image=image, attrs=attrs)
    container.configure_mock(name="running_container", should_exist=True)
    container.top.return_value = {
        "Titles": ["PID"],
        "Processes": [
            ['0'], ['1'], ['3']
        ]
    }

    return container


@pytest.fixture()
def finished_container():
    attrs = {
        "State": {
            "ExitCode": 0,
            "StartedAt": "2017-08-30T10:52:25.439434269Z",
            "Error": "",
            "FinishedAt": "2017-08-30T10:55:25.439434269Z"}
    }
    image = pytest.MagicMock(id='image_id', tags=['debian:latest'])
    container = pytest.Mock(
        id="bar",
        name="finished_container",
        status="exited",
        labels=["label"],
        image=image,
        attrs=attrs)
    container.configure_mock(name="finished_container", should_exist=True)

    return container
