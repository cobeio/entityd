"""Plugin providing entities for Docker.

This module implements all the entities for various Docker
components. Each entity type is implemented as a generator function.
A single ``entityd_find_entity`` hook implementation takes responsibility
for dispatching to the correct generator function.
"""

import logbook
import syskit
from docker import DockerClient
from docker.errors import DockerException

import entityd
from entityd.mixins import HostUEID

log = logbook.Logger(__name__)


class Client:
    """Helper class to cache and get Docker Client"""
    _client = None

    @classmethod
    def get_client(cls):
        """Get DockerClient for local system"""
        if not cls._client:
            try:
                cls._client = DockerClient(
                    base_url='unix://var/run/docker.sock',
                    timeout=5, version='auto')
            except DockerException:
                log.debug("Docker client not available")
                cls._client = None

        return cls._client

    @classmethod
    def client_available(cls):
        """Detects if docker is running on the local system"""
        if cls.get_client():
            return True

        return False


class DockerContainer:
    """Entity for a Docker Container"""
    name = "Docker:Container"

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs=None, include_ondemand=False):  # pylint: disable=unused-argument
        """Find Docker Container entities."""

        if name == self.name:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.generate_updates()

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Register the Process Monitored Entity."""
        config.addentity(self.name, 'entityd.docker.docker.DockerContainer')

    @classmethod
    def get_ueid(cls, container_id):
        """Get a docker container ueid"""
        entity = entityd.EntityUpdate(cls.name)
        entity.attrs.set('id', container_id, traits={'entity:id'})
        return entity.ueid

    def generate_updates(self):
        """Generate entity update objects for each container"""
        if not Client.client_available():
            return

        client = Client.get_client()
        daemon_ueid = DockerDaemon.get_ueid(client.info()['ID'])

        for container in client.containers.list(all=True):
            attrs = container.attrs

            update = entityd.EntityUpdate(self.name)
            update.label = container.name
            update.attrs.set('id', container.id, traits={'entity:id'})
            update.attrs.set('name', container.name)
            update.attrs.set('state:status', container.status)
            update.attrs.set('image:id', container.image.id)
            update.attrs.set('image:name', container.image.tags)
            update.attrs.set('labels', container.labels)

            update.attrs.set('state:started-at', attrs['State']['StartedAt'],
                             traits={'chrono:rfc3339'})

            if container.status == "exited" or container.status == "dead":
                update.exists = False
                update.attrs.set('state:exit-code', attrs['State']['ExitCode'])
                update.attrs.set('state:error', attrs['State']['Error'])
                update.attrs.set('state:finished-at',
                                 attrs['State']['FinishedAt'],
                                 traits={'chrono:rfc3339'})
            else:
                update.exists = True
                update.attrs.set('state:exit-code', None)
                update.attrs.set('state:error', None)
                update.attrs.set('state:finished-at', None)

            update.parents.add(daemon_ueid)

            yield update


class DockerContainerProcessGroup(HostUEID):
    """Entity for a grouping of Processes running within a Docker Container"""
    name = "Group"

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Register the Process Monitored Entity."""
        config.addentity(self.name,
                         'entityd.docker.docker.DockerContainerProcessGroup')

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs=None, include_ondemand=False):  # pylint: disable=unused-argument
        """Find the docker container process group entities"""
        if name == self.name:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.generate_updates()

    def get_process_ueid(self, pid):
        """Generate a ueid for a process.

        :param pid: A process id.

        :returns: A :class:`cobe.UEID` for the given process.
        """
        proc = syskit.Process(int(pid))

        entity = entityd.EntityUpdate('Process')
        entity.attrs.set('pid', proc.pid, traits={'entity:id'})
        entity.attrs.set('starttime', proc.start_time.timestamp(),
                         traits={'entity:id'})
        entity.attrs.set('host', str(self.host_ueid), traits={'entity:id'})

        return entity.ueid

    def get_missed_process_children(self, pid):
        """Use the process tree to get any PID's that might of been
        missed by docker.container.top()

        :param pid: A process id.

        :returns a generator of found pid's
        """
        proc = syskit.Process(int(pid))
        for child_proc in proc.children():
            yield child_proc.pid
            yield from self.get_missed_process_children(child_proc.pid)

    def generate_updates(self):
        """Generates the entity updates for the process group"""
        if not Client.client_available():
            return

        client = Client.get_client()
        for container in client.containers.list():
            if container.status != "running" and not container.top():
                continue

            update = entityd.EntityUpdate(self.name)
            update.label = container.name
            update.attrs.set(
                'kind', DockerContainer.name, traits={'entity:id'})
            container_ueid = DockerContainer.get_ueid(container.id)
            update.attrs.set('id', str(container_ueid), traits={'entity:id'})
            update.children.add(DockerContainer.get_ueid(container.id))

            top_results = container.top(ps_args="-o pid")

            processes = top_results['Processes']
            for process in processes:
                update.children.add(self.get_process_ueid(int(process[0])))

            if processes:
                for missed_pid in self.get_missed_process_children(
                        processes[0][0]):
                    update.children.add(
                        self.get_process_ueid(missed_pid))

            yield update


class DockerDaemon(HostUEID):
    """An entity for the docker daemon"""
    name = "Docker:Daemon"

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Register the Process Monitored Entity."""
        config.addentity(self.name, 'entityd.docker.docker.DockerDaemon')

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs=None,
                            include_ondemand=False):  # pylint: disable=unused-argument
        """Find the docker daemon entity"""

        if name == self.name:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.generate_updates()

    @classmethod
    def get_ueid(cls, docker_daemon_id):
        """Create a ueid for a docker daemon"""
        entity = entityd.EntityUpdate(cls.name)
        entity.attrs.set('id', docker_daemon_id, traits={'entity:id'})
        return entity.ueid

    def generate_updates(self):
        """Generates the entity updates for the docker daemon"""
        if Client.client_available():
            client = Client.get_client()
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



