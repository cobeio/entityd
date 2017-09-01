import logbook
from docker import DockerClient
from docker.errors import DockerException
from syskit._process import Process

import entityd
from entityd.mixins import HostUEID

log = logbook.Logger(__name__)


class Client:
    _client = None

    @classmethod
    def get_client(cls):
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
        if cls.get_client():
            return True

        return False


class DockerContainer:
    name = "Docker:Container"

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs=None, include_ondemand=False):  # pylint: disable=unused-argument
        if name == self.name:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.generate_updates()

    @entityd.pm.hookimpl
    def entityd_configure(cls, config):
        """Register the Process Monitored Entity."""
        config.addentity(cls.name, 'entityd.docker.docker.DockerContainer')

    @classmethod
    def get_ueid(cls, id):
        entity = entityd.EntityUpdate(cls.name)
        entity.attrs.set('id', id, traits={'entity:id'})
        return entity.ueid

    def generate_updates(self):
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
    name = "Group"

    @entityd.pm.hookimpl
    def entityd_configure(cls, config):
        """Register the Process Monitored Entity."""
        config.addentity(cls.name, 'entityd.docker.docker.DockerContainerProcessGroup')

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs=None, include_ondemand=False):  # pylint: disable=unused-argument
        if name == self.name:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.generate_updates()

    def get_process_ueid(self, pid):
        """Generate a ueid for a process.

        :param pid: A process id.

        :returns: A :class:`cobe.UEID` for the given process.
        """
        proc = Process(int(pid))

        entity = entityd.EntityUpdate('Process')
        entity.attrs.set('pid', proc.pid, traits={'entity:id'})
        entity.attrs.set('starttime', proc.start_time.timestamp(),
                         traits={'entity:id'})
        entity.attrs.set('host', str(self.host_ueid), traits={'entity:id'})

        return entity.ueid

    def get_missed_process_children(self, pid, already_added_pids):
        proc = Process(int(pid))
        for child_proc in proc.children():
            if child_proc.pid not in already_added_pids:
                yield child_proc.pid

            yield from self.get_missed_process_children(child_proc.pid, already_added_pids)

    def generate_updates(self):
        if not Client.client_available():
            return

        client = Client.get_client()
        for container in client.containers.list():
            if container.status == "running" and container.top(ps_args="-o pid"):
                update = entityd.EntityUpdate(self.name)
                update.label = container.name
                update.attrs.set('kind', DockerContainer.name, traits={'entity:id'})
                container_ueid = DockerContainer.get_ueid(container.id)
                update.attrs.set('ownerUEID', container_ueid, traits={'entity:id'})
                update.children.add(DockerContainer.get_ueid(container.id))

                top_results = container.top(ps_args="-o pid")

                added_pids = set()
                processes = top_results['Processes']
                for process in processes:
                    pid = int(process[0])
                    added_pids.add(pid)
                    update.children.add(self.get_process_ueid(pid))

                if processes:
                    for missed_pid in self.get_missed_process_children(
                            processes[0][0], added_pids):
                        update.children.add(
                            self.get_process_ueid(missed_pid))
                        added_pids.add(missed_pid)

                yield update


class DockerDaemon(HostUEID):
    name = "Docker:Daemon"

    @entityd.pm.hookimpl
    def entityd_configure(cls, config):
        """Register the Process Monitored Entity."""
        config.addentity(cls.name, 'entityd.docker.docker.DockerDaemon')

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs=None,
                            include_ondemand=False):  # pylint: disable=unused-argument
        if name == self.name:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.generate_updates()

    @classmethod
    def get_ueid(cls, id):
        entity = entityd.EntityUpdate(cls.name)
        entity.attrs.set('id', id, traits={'entity:id'})
        return entity.ueid

    def generate_updates(self):
        if Client.client_available():
            client = Client.get_client()
            dd_info = client.info()

            update = entityd.EntityUpdate(self.name)
            update.label = dd_info['Name']
            update.attrs.set('id', dd_info['ID'], traits={'entity:id'})
            update.attrs.set('containers:total', dd_info['Containers'])
            update.attrs.set('containers:paused', dd_info['ContainersPaused'])
            update.attrs.set('containers:running', dd_info['ContainersRunning'])
            update.attrs.set('containers:stopped', dd_info['ContainersStopped'])
            update.parents.add(self.host_ueid)

            yield update



