"""Plugin to provide docker swarm entities.

If a machine running docker is part of a swarm, a swarm
entity will be generated
"""

import logbook

import entityd
from entityd.docker.client import DockerClient

log = logbook.Logger(__name__)


class DockerSwarm:
    """An entity for the docker swarm."""
    name = 'Docker:Swarm'

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

    def generate_updates(self):
        """Generates the entity updates for the docker swarm."""
        if not DockerClient.client_available():
            return

        client = DockerClient.get_client()
        client_info = DockerClient.info()

        if DockerClient.swarm_exists() and DockerClient.is_swarm_manager():
            swarm_attrs = client_info['Swarm']
            swarm_spec = swarm_attrs['Cluster']['Spec']
            swarm_spec_raft = swarm_spec['Raft']

            update = entityd.EntityUpdate(self.name)
            update.label = swarm_attrs['Cluster']['ID'][:10]
            update.attrs.set('id', swarm_attrs['Cluster']['ID'],
                             traits={'entity:id'})
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

            for node in client.nodes.list():
                update.children.add(entityd.docker.get_ueid(
                    'DockerNode', node.attrs['ID']))

            yield update


class DockerNode:
    """An entity for the docker node."""
    name = 'Docker:Node'

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

        if DockerClient.swarm_exists() and DockerClient.is_swarm_manager():
            for node in client.nodes.list():
                update = entityd.EntityUpdate(self.name)
                update.label = node.attrs['Description']['Hostname']
                update.attrs.set('id', node.attrs['ID'],
                                 traits={'entity:id'})

                update.attrs.set('role',
                                 node.attrs['Spec']['Role'])
                update.attrs.set('availability',
                                 node.attrs['Spec']['Availability'])
                update.attrs.set('labels',
                                 node.attrs['Spec']['Labels'])
                update.attrs.set('state',
                                 node.attrs['Status']['State'])
                update.attrs.set('address',
                                 node.attrs['Status']['Addr'])
                update.attrs.set('version',
                                 node.attrs['Version']['Index'])

                if 'ManagerStatus' in node.attrs:
                    manager_attrs = node.attrs['ManagerStatus']
                    update.attrs.set('manager:reachability',
                                     manager_attrs['Reachability'])
                    update.attrs.set('manager:addr',
                                     manager_attrs['Addr'])
                    if 'Leader' in manager_attrs:
                        update.attrs.set('manager:leader',
                                         manager_attrs['Leader'])
                    else:
                        update.attrs.set('manager:leader', False)
                else:
                    update.attrs.set('manager:reachability', None)
                    update.attrs.set('manager:leader', None)
                    update.attrs.set('manager:addr', None)

                yield update


class DockerService:
    """An entity for the docker service."""
    name = "Docker:Service"

    def __init__(self):
        self._services = {}
        self._service_tasks = {}
        self._service_container_ids = {}
        self._service_container_states = {}

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Register the Docker Service Entity."""
        config.addentity(self.name, 'entityd.docker.swarm.DockerService')

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs=None,
                            include_ondemand=False):  # pylint: disable=unused-argument
        """Find the docker service entities."""
        if name == self.name:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.generate_updates()

    @entityd.pm.hookimpl
    def entityd_collection_before(self, session):  # pylint: disable=unused-argument
        """Collect services from available Docker daemon.

        If no connection to a Docker daemon is available this
        does nothing.
        """
        if (not DockerClient.client_available() or
                not DockerClient.swarm_exists() or
                not DockerClient.is_swarm_manager()):
            return
        client = DockerClient.get_client()
        self._services = {service.id: service for
                          service in client.services.list()}

        for service in self._services.values():
            self._service_tasks[service.id] = list(service.tasks())

        for service in self._services.values():
            self._service_container_ids[service.id] = \
                self.get_container_ids(service)

        for service in self._services.values():
            self._service_container_states[service.id] = \
                self.get_container_states(service)

    @entityd.pm.hookimpl
    def entityd_collection_after(self, session, updates):  # pylint: disable=unused-argument
        """Clear services that were collected during collection."""
        self._services.clear()
        self._service_tasks.clear()
        self._service_container_ids.clear()
        self._service_container_states.clear()

    @classmethod
    def get_ueid(cls, docker_node_id):
        """Create a ueid for a docker service."""
        entity = entityd.EntityUpdate(cls.name)
        entity.attrs.set('id', docker_node_id, traits={'entity:id'})
        return entity.ueid

    def get_container_ids(self, service):
        """Returns a set of container ids for a service."""
        container_ids = set()
        for task in self._service_tasks[service.id]:
            task_status = task['Status']
            if ('ContainerStatus' in task_status and
                    'ContainerID' in task_status['ContainerStatus']):
                container_id = task_status['ContainerStatus']['ContainerID']
                container_ids.add(container_id)
        return container_ids

    def get_container_states(self, service):
        """Returns a list of container states."""
        container_states = list()
        for task in self._service_tasks[service.id]:
            container_states.append(task['Status']['State'])
        return container_states

    def populate_mode_fields(self, mode_attrs, update):
        """Add fields depending on the service mode."""
        if "Replicated" in mode_attrs:
            update.attrs.set('mode', 'replicated')
            update.attrs.set('replicas:desired',
                             mode_attrs['Replicated']['Replicas'])
        elif "Global" in mode_attrs:
            update.attrs.set('mode', 'global')

    def populate_task_fields(self, service, update):
        """Add fields for the tasks of a service."""
        possible_states = ['new', 'pending', 'assigned', 'accepted',
                           'preparing', 'ready', 'starting', 'running',
                           'complete', 'shutdown', 'failed', 'rejected',
                           'orphaned']
        totals = {state: 0 for state in possible_states}
        for state in \
                self._service_container_states[service.id]:
            totals[state] += 1

        for key, value in totals.items():
            update.attrs.set('replicas:' + key, value)

    def add_mount_relationships(self, service, mount, update):
        """Adds relationships based on the services mounts"""
        if mount['Type'] == 'volume':
            volume_ueid = entityd.docker.get_ueid(
                'DockerVolume',
                DockerClient.info()['ID'],
                mount['Source'])
            update.children.add(volume_ueid)
            for container_id in \
                    self._service_container_ids[service.id]:
                mount_ueid = entityd.docker.get_ueid(
                    'DockerVolumeMount',
                    mount['Target'],
                    container_id)
                update.children.add(mount_ueid)

    def populate_service_fields(self, service):
        """Creates an EntityUpdate object for a docker service."""
        service_spec = service.attrs['Spec']
        update = entityd.EntityUpdate(self.name)
        update.label = service_spec['Name']
        update.attrs.set('id', service.attrs['ID'], traits={'entity:id'})
        update.attrs.set('labels', service_spec['Labels'])
        self.populate_mode_fields(service_spec['Mode'], update)
        self.populate_task_fields(service, update)

        task_template = service_spec['TaskTemplate']

        if 'Networks' in task_template:
            for network in task_template['Networks']:
                network_ueid = entityd.docker.get_ueid(
                    'DockerNetwork', network['Target'])
                update.parents.add(network_ueid)

        if 'Mounts' in task_template['ContainerSpec']:
            for mount in task_template['ContainerSpec']['Mounts']:
                self.add_mount_relationships(service, mount, update)

        for container_id in self._service_container_ids[service.id]:
            container_ueid = entityd.docker.get_ueid(
                'DockerContainer', container_id)
            update.children.add(container_ueid)

        return update

    def generate_updates(self):
        """Generates the entity updates for the docker service."""
        if not DockerClient.client_available():
            return

        client_info = DockerClient.info()

        if DockerClient.swarm_exists() and DockerClient.is_swarm_manager():
            for service in self._services.values():
                update = self.populate_service_fields(service)

                swarm_id = client_info['Swarm']['Cluster']['ID']
                swarm_ueid = entityd.docker.get_ueid(
                    'DockerSwarm', swarm_id)
                update.parents.add(swarm_ueid)

                yield update


class DockerNetwork:
    """An entity for the docker network."""
    name = "Docker:Network"

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Register the Docker Network Entity."""
        config.addentity(self.name, 'entityd.docker.swarm.DockerNetwork')

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs=None,
                            include_ondemand=False):  # pylint: disable=unused-argument
        """Find the docker network entities."""
        if name == self.name:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.generate_updates()

    @classmethod
    def get_ueid(cls, docker_network_id):
        """Create a ueid for a docker network."""
        entity = entityd.EntityUpdate(cls.name)
        entity.attrs.set('id', docker_network_id, traits={'entity:id'})
        return entity.ueid

    def populate_network_fields(self, network):
        """Create EntityUpdate for a docker network."""
        update = entityd.EntityUpdate(self.name)
        update.label = network.attrs['Name']
        update.attrs.set('id', network.id, traits={'entity:id'})
        update.attrs.set('labels', network.attrs['Labels'])
        update.attrs.set('options', network.attrs['Options'])
        update.attrs.set('driver', network.attrs['Driver'])
        update.attrs.set('ipv6-enabled', network.attrs['EnableIPv6'])
        update.attrs.set('ingress', network.attrs['Ingress'])
        update.attrs.set('internal', network.attrs['Internal'])
        update.attrs.set('scope', network.attrs['Scope'])

        return update

    def generate_updates(self):
        """Generates the entity updates for the docker network."""
        if not DockerClient.client_available():
            return

        client = DockerClient.get_client()
        client_info = DockerClient.info()

        swarm_ueid = None
        if DockerClient.swarm_exists() and DockerClient.is_swarm_manager():
            swarm_id = client_info['Swarm']['Cluster']['ID']
            swarm_ueid = entityd.docker.get_ueid('DockerSwarm', swarm_id)

        daemon_ueid = entityd.docker.get_ueid('DockerDaemon',
                                              client_info['ID'])

        for network in client.networks.list():
            update = None
            if network.attrs['Scope'] == "swarm" and swarm_ueid:
                update = self.populate_network_fields(network)
                update.parents.add(swarm_ueid)
            elif network.attrs['Scope'] == "local":
                update = self.populate_network_fields(network)
                update.parents.add(daemon_ueid)

            if update:
                yield update
