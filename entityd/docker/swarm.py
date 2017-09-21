"""Plugin to provide docker swarm entities.

If a machine running docker is part of a swarm, a swarm
entity will be generated
"""

import logbook
from docker.errors import APIError

import entityd
from entityd.docker.client import DockerClient

log = logbook.Logger(__name__)


class DockerSwarm:
    """An entity for the docker swarm."""
    name = "Docker:Swarm"

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Register the Process Monitored Entity."""
        config.addentity(self.name, 'entityd.docker.swarm.DockerSwarm')

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs=None,
                            include_ondemand=False):  # pylint: disable=unused-argument
        """Find the docker swarm entity."""

        if name == self.name:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.generate_updates()

    @classmethod
    def get_ueid(cls, docker_swarm_id):
        """Create a ueid for a docker swarm."""
        entity = entityd.EntityUpdate(cls.name)
        entity.attrs.set('id', docker_swarm_id, traits={'entity:id'})
        return entity.ueid

    @classmethod
    def swarm_exists(cls, client_info):
        """Checks if the docker client is connected to a docker swarm."""
        if client_info['Swarm']['LocalNodeState'] == "active":
            return True
        return False

    def generate_updates(self):
        """Generates the entity updates for the docker swarm."""
        if not DockerClient.client_available():
            return

        client = DockerClient.get_client()
        client_info = client.info()

        if self.swarm_exists(client_info):
            swarm_attrs = client_info['Swarm']
            swarm_spec = swarm_attrs['Cluster']['Spec']
            swarm_spec_raft = swarm_spec['Raft']

            update = entityd.EntityUpdate(self.name)
            update.label = client.swarm.short_id
            update.attrs.set('id', client.swarm.id, traits={'entity:id'})
            update.attrs.set('control-available',
                             swarm_attrs['ControlAvailable'])

            update.attrs.set('error', swarm_attrs['Error'])

            update.attrs.set('nodes:total', swarm_attrs['Nodes'],
                             traits={'metric:gauge'})
            update.attrs.set('nodes:managers', swarm_attrs['Managers'],
                             traits={'metric:gauge'})

            update.attrs.set('name', swarm_spec['Name'])
            update.attrs.set(
                'auto-lock-managers',
                swarm_spec['EncryptionConfig']['AutoLockManagers'])

            update.attrs.set('raft:election-tick',
                             swarm_spec_raft['ElectionTick'])
            update.attrs.set('raft:heartbeat-tick',
                             swarm_spec_raft['HeartbeatTick'])
            update.attrs.set('raft:keep-old-snapshots',
                             swarm_spec_raft['KeepOldSnapshots'])
            update.attrs.set(
                'raft:log-entries-for-slow-followers',
                swarm_spec_raft['LogEntriesForSlowFollowers'])
            update.attrs.set('raft:snapshot-interval',
                             swarm_spec_raft['SnapshotInterval'])

            try:
                for node in client.nodes.list():
                    update.children.add(DockerNode.get_ueid(node.attrs['ID']))
            except APIError as error:
                if error.status_code == 503:
                    log.debug("Can't get node list on non manager nodes")
                else:
                    raise

            yield update


class DockerNode:
    """An entity for the docker node."""
    name = "Docker:Node"

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Register the Docker Node Entity."""
        config.addentity(self.name, 'entityd.docker.swarm.DockerNode')

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs=None,
                            include_ondemand=False):  # pylint: disable=unused-argument
        """Find the docker node entities."""
        if name == self.name:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.generate_updates()

    @classmethod
    def get_ueid(cls, docker_node_id):
        """Create a ueid for a docker node."""
        entity = entityd.EntityUpdate(cls.name)
        entity.attrs.set('id', docker_node_id, traits={'entity:id'})
        return entity.ueid

    def generate_updates(self):
        """Generates the entity updates for the docker node."""
        if not DockerClient.client_available():
            return

        client = DockerClient.get_client()
        client_info = client.info()

        if DockerSwarm.swarm_exists(client_info):
            try:
                for node in client.nodes.list():
                    update = entityd.EntityUpdate(self.name)
                    update.label = node.attrs['Description']['Hostname']
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
