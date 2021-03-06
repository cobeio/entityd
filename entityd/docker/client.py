"""Module contains class for connecting to the docker client."""

import docker
import docker.errors
import logbook

import entityd

log = logbook.Logger(__name__)


class DockerClient:
    """Helper class to cache and get Docker DockerClient."""
    _client = None
    _client_info = None
    _all_containers = None

    @classmethod
    def get_client(cls):
        """Get DockerClient for local system"""
        if not cls._client:
            try:
                cls._client = docker.DockerClient(
                    base_url='unix://var/run/docker.sock',
                    timeout=5, version='auto')
            except docker.errors.DockerException:
                log.debug("Docker client not available")
                cls._client = None

        return cls._client

    @classmethod
    def client_available(cls):
        """Detects if docker is running on the local system."""
        if cls.get_client():
            return True

        return False

    @entityd.pm.hookimpl
    def entityd_collection_after(self, session):  # pylint: disable=unused-argument
        """Clear the client_info before the entityd collection starts."""
        DockerClient._client_info = None
        DockerClient._all_containers = None

    @classmethod
    def info(cls):
        """Docker client information.

        Lazy loads the client info from docker and stores for all subsequent
        requests this collection cycle.
        """

        if not cls._client_info:
            cls._client_info = cls.get_client().info()
        return cls._client_info

    @classmethod
    def swarm_exists(cls):
        """Checks if the docker client is connected to a docker swarm."""
        info = cls.info()
        if 'Swarm' in info and 'LocalNodeState' in info['Swarm']:
            return info['Swarm']['LocalNodeState'] == 'active'
        return False

    @classmethod
    def is_swarm_manager(cls):
        """Is the current node a swarm manager"""
        if cls.swarm_exists():
            swarm_info = cls.info()['Swarm']
            node_id = swarm_info['NodeID']
            for manager in swarm_info['RemoteManagers']:
                if node_id == manager['NodeID']:
                    return True
        return False

    @classmethod
    def all_containers(cls):
        """Returns all docker containers.

        This will return the same list of containers until
        the end of the collection cycle. If there is a docker exception an
        empty list will be returned.
        """
        if cls._all_containers is None:
            try:
                cls._all_containers = list(
                    cls.get_client().containers.list(all=True))
            except docker.errors.DockerException as error:
                # We set the list to be an empty list so further calls to
                # this function get the same result until the end of this
                # collection cycle
                log.debug(
                    "DockerException caught when getting containers {}.",
                    error
                )
                cls._all_containers = []

        return cls._all_containers

    @classmethod
    def running_containers(cls):
        """Returns all running docker containers."""
        return [x for x in cls.all_containers() if x.status == "running"]
