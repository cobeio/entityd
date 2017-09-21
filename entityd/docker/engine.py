import entityd
from entityd.docker.client import DockerClient
from entityd.docker.swarm import DockerSwarm
from entityd.mixins import HostEntity


class DockerEngine(HostEntity):
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
            client_info = client.info()

            update = entityd.EntityUpdate(self.name)
            update.label = client_info['Name']
            update.attrs.set('id', client_info['ID'], traits={'entity:id'})
            update.attrs.set('containers:total', client_info['Containers'])
            update.attrs.set('containers:paused', client_info['ContainersPaused'])
            update.attrs.set(
                'containers:running', client_info['ContainersRunning'])
            update.attrs.set(
                'containers:stopped', client_info['ContainersStopped'])
            update.parents.add(self.host_ueid)

            if DockerSwarm.swarm_exists():
                node_address = client_info['Swarm']['NodeAddr']
                found_node = next(
                    (node for node in client.nodes.list()
                     if node.attrs['Status']['Addr'] == node_address), None)
                if found_node:
                    update.attrs.set('node:role',
                                     found_node.attrs['Spec']['Role'])
                    update.attrs.set('node:availability',
                                     found_node.attrs['Spec']['Availability'])
                    update.attrs.set('node:labels',
                                     found_node.attrs['Spec']['Labels'])
                    update.attrs.set('node:state',
                                     found_node.attrs['Status']['State'])


            yield update
