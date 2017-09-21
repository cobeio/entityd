"""Module contains class for connecting to the docker client."""

import docker
import docker.errors
import logbook

log = logbook.Logger(__name__)


class DockerClient:
    """Helper class to cache and get Docker DockerClient."""
    _client = None

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
