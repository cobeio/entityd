"""This package contains all the docker entities."""
import abc

import logbook

import entityd

log = logbook.Logger(__name__)


def get_ueid(class_name, *args):
    """Get a ueid for any docker entity type."""
    from entityd.docker.container import DockerContainer
    from entityd.docker.image import DockerImage
    from entityd.docker.swarm import DockerSecret
    from entityd.docker.swarm import DockerService
    from entityd.docker.swarm import DockerSwarm
    from entityd.docker.daemon import DockerDaemon
    from entityd.docker.swarm import DockerNetwork
    from entityd.docker.swarm import DockerNode
    from entityd.docker.volume import DockerVolume
    from entityd.docker.volume import DockerVolumeMount

    values = {x.__name__: x for x in [
        DockerContainer,
        DockerImage,
        DockerSecret,
        DockerService,
        DockerSwarm,
        DockerDaemon,
        DockerNetwork,
        DockerNode,
        DockerVolume,
        DockerVolumeMount,
    ]}
    return values[class_name].get_ueid(*args)


class BaseDocker(metaclass=abc.ABCMeta):
    """Base docker class for registering entity types with entityd."""
    name = None

    @entityd.pm.hookimpl
    def entityd_emit_entities(self):
        """Generate all Docker daemon entity updates."""
        yield from self.generate_updates()

    @abc.abstractmethod
    def generate_updates(self):
        """Override this method to generate entity updates """
        pass
