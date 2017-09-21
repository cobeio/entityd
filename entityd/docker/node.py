import logbook
from docker.errors import APIError

import entityd
from entityd.docker.client import DockerClient
from entityd.docker.swarm import DockerSwarm

log = logbook.Logger(__name__)


class DockerNode():
    """An entity for the docker daemon"""
    name = "Docker:Node"

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Register the Process Monitored Entity."""
        config.addentity(self.name, 'entityd.docker.node.DockerNode')

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs=None,
                            include_ondemand=False):  # pylint: disable=unused-argument
        """Find the docker daemon entity"""
        if name == self.name:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.generate_updates()

    @classmethod
    def get_ueid(cls, docker_node_id):
        """Create a ueid for a docker daemon"""
        entity = entityd.EntityUpdate(cls.name)
        entity.attrs.set('id', docker_node_id, traits={'entity:id'})
        return entity.ueid

    def generate_updates(self):
        """Generates the entity updates for the docker daemon"""
        if DockerClient.client_available():
            client = DockerClient.get_client()
            client_info = client.info()

            if DockerSwarm.swarm_exists():
                try:
                    for node in client.nodes.list():
                        update = entityd.EntityUpdate(self.name)
                        update.label = node.attrs['HostName']
                        update.attrs.set('id', node.attrs['ID'],
                                         traits={'entity:id'})

                        update.attrs.set('node:id', node.attrs['ID'])
                        update.attrs.set('node:role',
                                         node.attrs['Spec']['Role'])
                        update.attrs.set('node:availability',
                                         node.attrs['Spec']['Availability'])
                        update.attrs.set('node:labels',
                                         node.attrs['Spec']['Labels'])
                        update.attrs.set('node:state',
                                         node.attrs['Status']['State'])
                        update.attrs.set('node:address',
                                         node.attrs['Status']['Addr'])
                        update.attrs.set('node:version',
                                         node.attrs['Version']['Index'])

                        manager_attrs = node.attrs['ManagerStatus']
                        update.attrs.set('node:manager:reachability',
                                         manager_attrs['Reachability'])
                        update.attrs.set('node:manager:leader',
                                         manager_attrs['Leader'])
                        update.attrs.set('node:manager:addr',
                                         manager_attrs['Addr'])
                        yield update
                except APIError as error:
                    if error.status_code == 503:
                        log.debug("Can't get node list on non manager nodes")
                    else:
                        raise
