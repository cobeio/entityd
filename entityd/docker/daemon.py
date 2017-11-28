"""Plugin to provide docker daemon entities.

For each machine running a docker daemon an entity
will be generated
"""

import entityd
from entityd.docker.client import DockerClient
from entityd.mixins import HostEntity


class DockerDaemon(HostEntity):
    """An entity for the docker daemon."""
    name = 'Docker:Daemon'

    @entityd.pm.hookimpl
    def entityd_emit_entities(self):
        """Generate all Docker daemon entity updates."""
        yield from self._generate_daemon()

    @classmethod
    def get_ueid(cls, docker_daemon_id):
        """Create a ueid for a docker daemon."""
        entity = entityd.EntityUpdate(cls.name)
        entity.attrs.set('id', docker_daemon_id, traits={'entity:id'})
        return entity.ueid

    def _generate_daemon(self):
        """Generates the entity update for the docker daemon."""
        if DockerClient.client_available():
            client_info = DockerClient.info()

            update = entityd.EntityUpdate(self.name)
            update.label = client_info['Name']
            update.attrs.set('id', client_info['ID'], traits={'entity:id'})
            update.attrs.set('containers:total', client_info['Containers'])
            update.attrs.set(
                'containers:paused', client_info['ContainersPaused'])
            update.attrs.set(
                'containers:running', client_info['ContainersRunning'])
            update.attrs.set(
                'containers:stopped', client_info['ContainersStopped'])
            update.parents.add(self.host_ueid)

            if DockerClient.swarm_exists():
                node_ueid = entityd.docker.get_ueid(
                    'DockerNode', client_info['Swarm']['NodeID'])
                update.parents.add(node_ueid)
            yield from self._generate_label_entities(client_info['Labels'],
                                                     update)
            yield update

    def _generate_label_entities(self, labels, update):
        """Generate update for a Docker label."""
        for label in labels:
            label_entity = entityd.EntityUpdate('Group')
            label_entity.label = label
            args = label.split('=')
            if len(args) == 2:
                label_entity.attrs.set('kind', 'label:' + args[0],
                                       {'entity:id'})
                label_entity.attrs.set('id', args[1], {'entity:id'})
            else:
                label_entity.attrs.set('kind', 'label:' + label, {'entity:id'})
            label_entity.children.add(update)
            yield label_entity
