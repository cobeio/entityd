import entityd
from entityd.docker.client import DockerClient


class DockerSwarm():
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

    @classmethod
    def swarm_exists(cls):
        if DockerClient.client_available():
            client = DockerClient.get_client()
            client_info = client.info()
            if client_info['Swarm']['LocalNodeState'] == "active":
                return True
        return False


    def generate_updates(self):
        """Generates the entity updates for the docker daemon"""
        if self.swarm_exists():
            client = DockerClient.get_client()
            client_info = client.info()

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

            update.attrs.set('specification:name', swarm_spec['Name'])
            update.attrs.set(
                'specification:auto-lock-managers',
                swarm_spec['EncryptionConfig']['AutoLockManagers'])

            update.attrs.set('specification:raft:election-tick',
                             swarm_spec_raft['ElectionTick'])
            update.attrs.set('specification:raft:heartbeat-tick',
                             swarm_spec_raft['HeartbeatTick'])
            update.attrs.set('specification:raft:keep-old-snapshots',
                             swarm_spec_raft['KeepOldSnapshots'])
            update.attrs.set(
                'specification:raft:log-entries-for-slow-followers',
                swarm_spec_raft['LogEntriesForSlowFollowers'])
            update.attrs.set('specification:raft:snapshot-interval',
                             swarm_spec_raft['SnapshotInterval'])

            yield update