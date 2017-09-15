"""
Plugin to provide container group entities

Each running container will have a container group with
a child for each process.
"""
import logbook
import syskit

import entityd
from entityd.docker.client import DockerClient
from entityd.docker.container import DockerContainer
from entityd.mixins import HostUEID

log = logbook.Logger(__name__)


class DockerContainerGroup(HostUEID):
    """Entity for a grouping of Processes running within a Docker Container"""
    name = "Group"

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Register the Process Monitored Entity."""
        config.addentity(
            self.name,
            'entityd.docker.container_group.DockerContainerGroup')

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
        try:
            proc = syskit.Process(int(pid))
        except syskit.NoSuchProcessError as error:
            log.warning("Process ({}) not found {}", pid, error)
        else:
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
        missed_processes = []
        try:
            proc = syskit.Process(int(pid))
        except syskit.NoSuchProcessError as error:
            log.warning("Process ({}) not found {}", pid, error)
        else:
            for child_proc in proc.children():
                missed_processes.append(child_proc.pid)
                missed_processes.extend(
                    self.get_missed_process_children(child_proc.pid))
        return missed_processes

    def generate_updates(self):
        """Generates the entity updates for the process group"""
        if not DockerClient.client_available():
            return

        client = DockerClient.get_client()
        for container in client.containers.list():
            if container.status != "running":
                continue

            top_results = container.top(ps_args="-o pid")
            processes = top_results['Processes']
            if not processes:
                continue

            update = entityd.EntityUpdate(self.name)
            update.label = container.name
            update.attrs.set(
                'kind', DockerContainer.name, traits={'entity:id'})
            container_ueid = DockerContainer.get_ueid(container.id)
            update.attrs.set('id', str(container_ueid), traits={'entity:id'})
            update.children.add(DockerContainer.get_ueid(container.id))

            for process in processes:
                process_ueid = self.get_process_ueid(int(process[0]))
                if process_ueid:
                    update.children.add(process_ueid)

            for missed_pid in self.get_missed_process_children(
                    processes[0][0]):
                process_ueid = self.get_process_ueid(missed_pid)
                if process_ueid:
                    update.children.add(process_ueid)

            yield update
