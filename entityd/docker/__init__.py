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
    def entityd_configure(self, config):
        """Register the Docker Entity."""
        cls = self.__class__
        config.addentity(self.name, cls.__module__ + "." + cls.__name__)
        log.info('Adding docker entity name {} ({})'.format(
            self.name, cls.__module__ + "." + cls.__name__))

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs=None,
                            include_ondemand=False):  # pylint: disable=unused-argument
        """Find the docker entities."""
        if name == self.name:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.generate_updates()

    @abc.abstractmethod
    def generate_updates(self):
        """Override this method to generate entity updates """
        pass
