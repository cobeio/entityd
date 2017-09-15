import entityd
from entityd.docker.client import DockerClient


class DockerSwarm(HostUEID):
    """An entity for the docker daemon"""
    name = "Docker:Swarm"

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Register the Process Monitored Entity."""
        config.addentity(self.name, 'entityd.docker.swarm.DockerSwarm')

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs=None,
                            include_ondemand=False):  # pylint: disable=unused-argument
        """Find the docker swarm entity"""

        if name == self.name:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.generate_updates()

    @classmethod
    def get_ueid(cls, docker_swarm_id):
        """Create a ueid for a docker daemon"""
        entity = entityd.EntityUpdate(cls.name)
        entity.attrs.set('id', docker_swarm_id, traits={'entity:id'})
        return entity.ueid

    def generate_updates(self):
        """Generates the entity updates for the docker daemon"""
        if DockerClient.client_available():
            client = DockerClient.get_client()
            if client.swarm.id:
                swarm_attrs = client.swarm.attrs

                update = entityd.EntityUpdate(self.name)
                update.label = client.swarm.short_id
                update.attrs.set('id', swarm_attrs['ID'], traits={'entity:id'})
                update.attrs.set('containers:total', dd_info['Containers'])
                update.attrs.set('containers:paused', dd_info['ContainersPaused'])
                update.attrs.set(
                    'containers:running', dd_info['ContainersRunning'])
                update.attrs.set(
                    'containers:stopped', dd_info['ContainersStopped'])
                update.parents.add(self.host_ueid)

                yield update