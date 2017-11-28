"""Plugin to provide docker swarm entities.

If a machine running docker is part of a swarm, a swarm
entity will be generated
"""

import stat

import logbook

import entityd
from entityd.docker.client import DockerClient

log = logbook.Logger(__name__)


class DockerSwarm:
    """An entity for the docker swarm."""
    name = 'Docker:Swarm'

    @entityd.pm.hookimpl
    def entityd_emit_entities(self):
        """Generate all Docker daemon entity updates."""
        yield from self.generate_updates()

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
            yield from self._generate_label_entities(swarm_spec, update)
            yield update

    def _generate_label_entities(self, swarm_spec, update):
        """Generate update for a Docker label."""
        try:
            for label_key, label_value in \
                    swarm_spec.get('Labels').items():
                label_entity = entityd.EntityUpdate('Group')
                label_entity.label = "{0} = {1}".format(label_key, label_value)
                label_entity.attrs.set('kind',
                                       'label:' + label_key,
                                       {'entity:id'},
                                      )
                label_entity.attrs.set('id', label_value, {'entity:id'})
                label_entity.children.add(update)
                yield label_entity
        except AttributeError:
            pass



class DockerNode:
    """An entity for the docker node."""
    name = 'Docker:Node'

    @entityd.pm.hookimpl
    def entityd_emit_entities(self):
        """Generate all Docker daemon entity updates."""
        yield from self.generate_updates()

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
                    update.attrs.set('manager:address',
                                     manager_attrs['Addr'])
                    if 'Leader' in manager_attrs:
                        update.attrs.set('manager:leader',
                                         manager_attrs['Leader'])
                    else:
                        update.attrs.set('manager:leader', False)
                else:
                    update.attrs.set('manager:reachability', None)
                    update.attrs.set('manager:leader', None)
                    update.attrs.set('manager:address', None)
                yield from self._generate_label_entities(node.attrs['Spec'],
                                                         update)
                yield update

    def _generate_label_entities(self, node_spec, update):
        """Generate update for a Docker label."""
        try:
            for label_key, label_value in \
                    node_spec.get('Labels').items():
                label_entity = entityd.EntityUpdate('Group')
                label_entity.label = "{0} = {1}".format(label_key, label_value)
                label_entity.attrs.set('kind',
                                       'label:' + label_key,
                                       {'entity:id'},
                                      )
                label_entity.attrs.set('id', label_value, {'entity:id'})
                label_entity.children.add(update)
                yield label_entity
        except AttributeError:
            pass


class DockerService:
    """An entity for the docker service."""
    name = "Docker:Service"

    def __init__(self):
        self._services = {}
        self._service_tasks = {}
        self._service_container_ids = {}
        self._service_container_states = {}

    @entityd.pm.hookimpl
    def entityd_emit_entities(self):
        """Generate all Docker daemon entity updates."""
        yield from self.generate_updates()

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
    def get_ueid(cls, docker_service_id):
        """Create a ueid for a docker service."""
        entity = entityd.EntityUpdate(cls.name)
        entity.attrs.set('id', docker_service_id, traits={'entity:id'})
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
        for state in self._service_container_states[service.id]:
            totals[state] += 1

        for key, value in totals.items():
            update.attrs.set('replicas:' + key, value)

    def add_mount_relationships(self, service, mount, update):
        """Adds relationships based on the services mounts."""
        if mount['Type'] == 'volume':
            volume_ueid = entityd.docker.get_ueid(
                'DockerVolume',
                DockerClient.info()['ID'],
                mount['Source'],
            )
            update.children.add(volume_ueid)
            for container_id in self._service_container_ids[service.id]:
                mount_ueid = entityd.docker.get_ueid(
                    'DockerVolumeMount',
                    mount['Target'],
                    container_id,
                )
                update.children.add(mount_ueid)

    def populate_service_fields(self, service):
        """Creates an EntityUpdate object for a docker service."""
        service_spec = service.attrs['Spec']
        update = entityd.EntityUpdate(self.name)
        update.label = service_spec['Name']
        update.attrs.set('id', service.attrs['ID'], traits={'entity:id'})
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
                yield from self._generate_label_entities(service.attrs['Spec'],
                                                         update)
                yield update

    def _generate_label_entities(self, service_spec, update):
        """Generate update for a Docker label."""
        try:
            for label_key, label_value in \
                    service_spec.get('Labels').items():
                label_entity = entityd.EntityUpdate('Group')
                label_entity.label = "{0} = {1}".format(label_key, label_value)
                label_entity.attrs.set('kind',
                                       'label:' + label_key,
                                       {'entity:id'},
                                      )
                label_entity.attrs.set('id', label_value, {'entity:id'})
                label_entity.children.add(update)
                yield label_entity
        except AttributeError:
            pass


class DockerNetwork:
    """An entity for the docker network."""
    name = "Docker:Network"

    @entityd.pm.hookimpl
    def entityd_emit_entities(self):
        """Generate all Docker daemon entity updates."""
        yield from self.generate_updates()

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
        update.attrs.set('options', network.attrs['Options'])
        update.attrs.set('driver', network.attrs['Driver'])
        update.attrs.set('ipv6-enabled', network.attrs['EnableIPv6'])
        update.attrs.set('ingress', network.attrs.get('Ingress'))
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
            if network.attrs['Scope'] == "swarm" and swarm_ueid:
                update = self.populate_network_fields(network)
                update.parents.add(swarm_ueid)
                yield from self._generate_label_entities(network, update)
                yield update
            elif network.attrs['Scope'] == "local":
                update = self.populate_network_fields(network)
                update.parents.add(daemon_ueid)
                yield from self._generate_label_entities(network, update)
                yield update

    def _generate_label_entities(self, network, update):
        """Generate update for a Docker label."""
        try:
            for label_key, label_value in \
                    network.attrs.get('Labels').items():
                label_entity = entityd.EntityUpdate('Group')
                label_entity.label = "{0} = {1}".format(label_key, label_value)
                label_entity.attrs.set('kind',
                                       'label:' + label_key,
                                       {'entity:id'},
                                      )
                label_entity.attrs.set('id', label_value, {'entity:id'})
                label_entity.children.add(update)
                yield label_entity
        except AttributeError:
            pass



class DockerSecret:
    """A plugin for Docker secrets."""

    _TYPES = {
        'Docker:Secret': '_generate_secrets',
        'Docker:Mount': '_generate_mounts',
    }

    def __init__(self):
        self._swarm_ueid = None
        self._secrets = {}  # id : secret
        self._services = []  # (service, tasks), ...

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs=None, include_ondemand=False):  # pylint: disable=unused-argument
        """Find Docker entities."""
        if name in self._TYPES:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            if self._swarm_ueid is not None:
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
        client_info = entityd.docker.client.DockerClient.info()
        if entityd.docker.client.DockerClient.is_swarm_manager():
            self._swarm_ueid = entityd.docker.get_ueid(
                'DockerSwarm', client_info['Swarm']['Cluster']['ID'])
            for secret in client.secrets.list():
                self._secrets[secret.id] = secret
            for service in client.services.list():
                self._services.append((service, list(service.tasks())))

    @entityd.pm.hookimpl
    def entityd_collection_after(self, session, updates):  # pylint: disable=unused-argument
        """Clear secrets that were collected during collection."""
        self._swarm_ueid = None
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
            update.parents.add(self._swarm_ueid)
            yield update

    def _generate_mounts(self):
        """Generate updates for all Docker secret mounts.

        :returns: Iterator of ``Docker:Mount`` updates.
        """
        for service, tasks in self._services:
            service_task = service.attrs['Spec']['TaskTemplate']
            if ('ContainerSpec' in service_task and
                    'Secrets' in service_task['ContainerSpec']):
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
                'secret:permissions',
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
