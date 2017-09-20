import entityd
from entityd.docker.client import DockerClient
from entityd.mixins import HostUEID


class DockerEngine(HostUEID):
    """An entity for the docker daemon"""
    name = "Docker:Engine"

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Register the Process Monitored Entity."""
        config.addentity(self.name, 'entityd.docker.daemon.DockerEngine')

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs=None,
                            include_ondemand=False):  # pylint: disable=unused-argument
        """Find the docker daemon entity"""

        if name == self.name:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.generate_updates()

    @classmethod
    def get_ueid(cls, docker_engine_id):
        """Create a ueid for a docker daemon"""
        entity = entityd.EntityUpdate(cls.name)
        entity.attrs.set('id', docker_engine_id, traits={'entity:id'})
        return entity.ueid

    def generate_updates(self):
        """Generates the entity updates for the docker daemon"""
        if DockerClient.client_available():
            client = DockerClient.get_client()
            dd_info = client.info()

            update = entityd.EntityUpdate(self.name)
            update.label = dd_info['Name']
            update.attrs.set('id', dd_info['ID'], traits={'entity:id'})
            update.attrs.set('containers:total', dd_info['Containers'])
            update.attrs.set('containers:paused', dd_info['ContainersPaused'])
            update.attrs.set(
                'containers:running', dd_info['ContainersRunning'])
            update.attrs.set(
                'containers:stopped', dd_info['ContainersStopped'])
            update.parents.add(self.host_ueid)

            yield update
