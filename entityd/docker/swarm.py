"""Plugin to provide docker swarm entities.

If a machine running docker is part of a swarm, a swarm
entity will be generated
"""

import stat

import logbook
from docker.errors import APIError

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

    @classmethod
    def swarm_exists(cls, client_info):
        """Checks if the docker client is connected to a docker swarm."""
        if client_info['Swarm']['LocalNodeState'] == 'active':
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

            try:
                for node in client.nodes.list():
                    update.children.add(entityd.docker.get_ueid(
                        'DockerNode', node.attrs['ID']))
            except APIError as error:
                if error.status_code == 503:
                    log.debug("Can't get node list on non manager nodes")
                else:
                    raise

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
        client_info = client.info()

        if DockerSwarm.swarm_exists(client_info):
            try:
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

                    manager_attrs = node.attrs['ManagerStatus']
                    update.attrs.set('manager:reachability',
                                     manager_attrs['Reachability'])
                    update.attrs.set('manager:leader',
                                     manager_attrs['Leader'])
                    update.attrs.set('manager:addr',
                                     manager_attrs['Addr'])
                    yield update
            except APIError as error:
                if error.status_code == 503:
                    log.debug("Can't get node list on non manager nodes")
                else:
                    raise


class DockerService:
    """An entity for the docker service."""
    name = "Docker:Service"

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

    @classmethod
    def get_ueid(cls, docker_node_id):
        """Create a ueid for a docker service."""
        entity = entityd.EntityUpdate(cls.name)
        entity.attrs.set('id', docker_node_id, traits={'entity:id'})
        return entity.ueid

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
        possible_states = ['pending', 'assigned', 'accepted', 'preparing',
                           'ready', 'starting', 'running', 'complete',
                           'shutdown', 'failed', 'rejected']
        totals = {state: 0 for state in possible_states}
        for task in service.tasks():
            task_status = task['Status']
            totals[task_status['State']] += 1
            if ('ContainerStatus' in task_status and
                    'ContainerID' in task_status['ContainerStatus']):
                container_id = task_status['ContainerStatus']['ContainerID']
                container_ueid = entityd.docker.get_ueid(
                    'DockerContainer', container_id)
                update.children.add(container_ueid)

        for key, value in totals.items():
            update.attrs.set('replicas:' + key, value)

    def populate_service_fields(self, service):
        """Creates an EntityUpdate object for a docker service."""
        update = entityd.EntityUpdate(self.name)
        update.label = service.attrs['Spec']['Name']
        update.attrs.set('id', service.attrs['ID'], traits={'entity:id'})
        update.attrs.set('labels', service.attrs['Spec']['Labels'])
        self.populate_mode_fields(service.attrs['Spec']['Mode'], update)
        self.populate_task_fields(service, update)

        if 'Networks' in service.attrs['Spec']['TaskTemplate']:
            for network in service.attrs['Spec']['TaskTemplate']['Networks']:
                network_ueid = entityd.docker.get_ueid(
                    'DockerNetwork', network['Target'])
                update.parents.add(network_ueid)

        return update

    def generate_updates(self):
        """Generates the entity updates for the docker service."""
        if not DockerClient.client_available():
            return

        client = DockerClient.get_client()
        client_info = client.info()

        if DockerSwarm.swarm_exists(client_info):
            try:
                for service in client.services.list():
                    update = self.populate_service_fields(service)

                    swarm_id = client_info['Swarm']['Cluster']['ID']
                    swarm_ueid = entityd.docker.get_ueid(
                        'DockerSwarm', swarm_id)
                    update.parents.add(swarm_ueid)

                    yield update
            except APIError as error:
                if error.status_code == 503:
                    log.debug("Can't get services list on non manager nodes")
                else:
                    raise


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
        client_info = client.info()

        swarm_ueid = None
        if DockerSwarm.swarm_exists(client_info):
            swarm_id = client_info['Swarm']['Cluster']['ID']
            swarm_ueid = entityd.docker.get_ueid('DockerSwarm', swarm_id)

        daemon_ueid = entityd.docker.get_ueid('DockerDaemon',
                                              client_info['ID'])

        for network in client.networks.list():
            update = self.populate_network_fields(network)
            if update.attrs.get('scope').value == "local":
                update.parents.add(daemon_ueid)
            elif update.attrs.get('scope').value == "swarm" and swarm_ueid:
                update.parents.add(swarm_ueid)

            yield update


class DockerSecret:
    """A plugin for Docker secrets."""

    _TYPES = {
        'Docker:Secret': '_generate_secrets',
        'Docker:Mount': '_generate_mounts',
    }

    def __init__(self):
        self._swarm = None
        self._secrets = {}  # id : secret
        self._services = []  # (service, tasks), ...

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs=None, include_ondemand=False):  # pylint: disable=unused-argument
        """Find Docker entities."""
        if name in self._TYPES:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            if self._swarm is not None:
                return getattr(self, self._TYPES[name])()
            else:
                log.info('Not collecting Docker secrets on non-manager node')

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Register the Process Monitored Entity."""
        for type_ in self._TYPES:
            config.addentity(
                type_, __name__ + '.' + self.__class__.__name__)

    @entityd.pm.hookimpl
    def entityd_collection_before(self, session):  # pylint: disable=unused-argument
        """Collect secrets from available Docker swarm.

        If no connection to a Docker Swarm manager daemon is available
        then this does nothing.
        """
        if not entityd.docker.client.DockerClient.client_available():
            return
        client = entityd.docker.client.DockerClient.get_client()
        client_info = client.info()
        if DockerSwarm.swarm_exists(client_info):
            self._swarm = entityd.docker.get_ueid(
                'DockerSwarm', client_info['Swarm']['Cluster']['ID'])
            for secret in client.secrets.list():
                self._secrets[secret.id] = secret
            for service in client.services.list():
                self._services.append((service, list(service.tasks())))

    @entityd.pm.hookimpl
    def entityd_collection_after(self, session, updates):  # pylint: disable=unused-argument
        """Clear secrets that were collected during collection."""
        self._swarm = None
        self._secrets.clear()
        del self._services[:]

    @classmethod
    def get_ueid(cls, id_):
        """Create a ueid for a docker network."""
        entity = entityd.EntityUpdate('Docker:Secret')
        entity.attrs.set('id', id_, traits={'entity:id'})
        return entity.ueid

    def _generate_secrets(self):
        """Generate updates for all Docker secrets.

        :returns: Iterator of ``Docker:Secret`` updates.
        """
        for secret in self._secrets.values():
            update = entityd.EntityUpdate('Docker:Secret')
            update.label = secret.name or secret.id
            update.attrs.set('id', secret.id, {'entity:id'})
            update.attrs.set('name', secret.name)
            update.attrs.set(
                'created', secret.attrs['CreatedAt'], {'time:rfc3339'})
            update.attrs.set(
                'updated', secret.attrs['UpdatedAt'], {'time:rfc3339'})
            update.parents.add(self._swarm)
            yield update

    def _generate_mounts(self):
        """Generate updates for all Docker secret mounts.

        :returns: Iterator of ``Docker:Mount`` updates.
        """
        for service, tasks in self._services:
            service_task = service.attrs['Spec']['TaskTemplate']
            if 'ContainerSpec' in service_task:
                for service_secret in service_task['ContainerSpec']['Secrets']:
                    for task in tasks:
                        yield from self._generate_mount(
                            service, task, service_secret)

    def _generate_mount(self, service, task, service_secret):
        """Generate a secret mount update.

        :param service: Docker service the mount is to be generated for.
        :type service: docker.models.services.Server
        :param task: Service task to generate the mount for.
        :type task: dict
        :param service_secret: Configured secret for the task to generate
            the mount update for.
        :type service_secret: dict

        :returns: Iterator of ``Docker:Mount`` updates.
        """
        task_status = task['Status']
        if ('ContainerStatus' in task_status and
                'ContainerID' in task_status['ContainerStatus']):
            update = entityd.EntityUpdate('Docker:Mount')
            update.label = service_secret['File']['Name']
            update.attrs.set('service', service.id, {'entity:id'})
            update.attrs.set(
                'container',
                task_status['ContainerStatus']['ContainerID'],
                {'entity:id'},
            )
            update.attrs.set(
                'target',
                '/var/secrets/' + service_secret['File']['Name'],
                {'entity:id'},
            )
            update.attrs.set(
                'permissions',
                stat.filemode(service_secret['File']['Mode']),
            )
            update.attrs.set('secret:gid', service_secret['File']['GID'])
            update.attrs.set('secret:uid', service_secret['File']['UID'])
            update.parents.add(self.get_ueid(service_secret['SecretID']))
            update.parents.add(DockerService.get_ueid(service.id))
            update.parents.add(entityd.docker.get_ueid(
                'DockerContainer',
                task_status['ContainerStatus']['ContainerID'],
            ))
            yield update
