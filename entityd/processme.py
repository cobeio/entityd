"""Plugin providing the Process Monitored Entity."""

import functools
import threading

import act
import docker
import logbook

import syskit
import zmq

import entityd.pm


class CpuUsage(threading.Thread):
    """A background thread to fetch CPU times and calculate percentages.

    Accessible via ZMQ Pair/Pair sockets; receiving a pid or ``None``,
    and returning the percentage cpu time calculated most recently
    for the given pid, or all known processes.

    :param Context context: The ZMQ context to use
    :param str endpoint: The ZMQ endpoint to listen for requests on
    :param int interval: The period in seconds to wait between refreshes

    :ivar last_run_process: A map of {pid->syskit.Process} from the
       last update.
    :ivar last_run_percentages: A map of {pid->float} percentage values.
    """
    def __init__(self, context, endpoint='inproc://cpuusage', interval=15):
        self._context = context
        self.listen_endpoint = endpoint
        self.last_run_processes = {}
        self.last_run_percentages = {}
        self._stream = None
        self._timer_interval = interval
        self._log = logbook.Logger('CpuUsage')
        super().__init__()

    @staticmethod
    def percent_cpu_usage(previous, current):
        """Return the percentage cpu time used since previous update.

        :param syskit.Process previous: The Process object from last update.
        :param syskit.Process current: The current Process object.
        """
        last_cpu_time = float(previous.cputime)
        last_clock_time = float(previous.refreshed.timestamp())
        cpu_time = float(current.cputime)
        clock_time = current.refreshed.timestamp()
        cpu_time_passed = cpu_time - last_cpu_time
        clock_time_passed = clock_time - last_clock_time
        if clock_time_passed == 0:
            return 0
        percent_cpu_usage = (cpu_time_passed / clock_time_passed) * 100
        return percent_cpu_usage

    def update(self):
        """Update the cpu percentage values we have stored."""
        new_percentages = {}
        new_processes = {}
        for pid in syskit.Process.enumerate():
            try:
                process = syskit.Process(pid)
            except syskit.NoSuchProcessError:
                continue
            else:
                try:
                    percentage = self.percent_cpu_usage(
                        self.last_run_processes[pid], process)
                except KeyError:
                    pass
                else:
                    new_percentages[pid] = percentage
                new_processes[pid] = process
        self.last_run_percentages = new_percentages
        self.last_run_processes = new_processes

    def run(self):
        while True:
            try:
                self._run()
            except Exception:  # pylint: disable=broad-except
                self._log.exception('An unexpected exception occurred in '
                                    'CpuUsage thread')
            else:
                break
            finally:
                self.stop()

    def _run(self):
        """Run the thread main loop.

        Registers the regular timer and polls for
        events from the incoming request socket.
        """
        self._stream = act.zkit.EventStream(self._context)
        timer = act.zkit.SimpleTimer()
        timer.schedule(0)
        sock = self._context.socket(zmq.PAIR)
        sock.bind(self.listen_endpoint)
        self._stream.register(sock, self._stream.POLLIN)
        self._stream.register(timer, self._stream.TIMER)
        try:
            for event, _ in self._stream:
                if event is timer:
                    self.update()
                    timer.schedule(self._timer_interval * 1000)
                elif event is sock:
                    pid = sock.recv_pyobj()
                    if pid is None:
                        response = self.last_run_percentages
                    else:
                        response = self.last_run_percentages.get(pid, None)
                    sock.send_pyobj(response)
        finally:
            sock.close(linger=0)
            self._stream.close()

    def stop(self):
        """Stop the thread safely.

        We have to consume the stream so that close() gets called.
        """
        if self._stream:
            self._stream.send_term()


class ProcessEntity:
    """Plugin to generate Process MEs."""

    prefix = 'entityd.processme:'

    def __init__(self):
        self.zmq_context = act.zkit.new_context()
        self.active_processes = {}
        self.session = None
        self._host_ueid = None
        self.cpu_usage_thread = None
        self.cpu_usage_sock = None
        try:
            self._docker_client = docker.Client(
                base_url='unix://var/run/docker.sock',
                timeout=3, version='auto')
        except docker.errors.DockerException:
            self._docker_client = None

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Register the Process Monitored Entity."""
        config.addentity('Process', 'entityd.processme.ProcessEntity')

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        """Store the session for later usage."""
        self.session = session
        self.cpu_usage_thread = CpuUsage(self.zmq_context)
        self.cpu_usage_thread.start()
        self.cpu_usage_sock = self.zmq_context.socket(zmq.PAIR)
        self.cpu_usage_sock.connect(self.cpu_usage_thread.listen_endpoint)

    @entityd.pm.hookimpl
    def entityd_sessionfinish(self):
        """Safely terminate the plugin."""
        if self.cpu_usage_thread:
            self.cpu_usage_thread.stop()
            self.cpu_usage_thread.join(timeout=2)
        if self.cpu_usage_sock:
            self.cpu_usage_sock.close(linger=0)
        self.zmq_context.destroy(linger=0)

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs, include_ondemand=False):  # pylint: disable=unused-argument
        """Return an iterator of "Process" Monitored Entities."""
        if name == 'Process':
            if attrs is not None:
                return self.filtered_processes(attrs)
            return self.processes()

    @property
    def host_ueid(self):
        """Property to get the host ueid, used in a few places.

        :raises LookupError: If a host UEID cannot be found.

        :returns: A :class:`cobe.UEID` for the  host.
        """
        if not self._host_ueid:
            results = self.session.pluginmanager.hooks.entityd_find_entity(
                name='Host', attrs=None)
            if results:
                for host_me in results[0]:
                    self._host_ueid = host_me.ueid
        if not self._host_ueid:
            raise LookupError('Could not find the host UEID')
        return self._host_ueid

    def get_ueid(self, proc):
        """Generate a ueid for this process.

        :param proc: syskit.Process instance.

        :returns: A :class:`cobe.UEID` for the given process.
        """
        entity = entityd.EntityUpdate('Process')
        entity.attrs.set('pid', proc.pid, traits={'entity:id'})
        entity.attrs.set('starttime', proc.start_time.timestamp(),
                         traits={'entity:id'})
        entity.attrs.set('host', str(self.host_ueid), traits={'entity:id'})
        return entity.ueid

    def get_parents(self, proc, procs):
        """Get relations for a process.

        Relations may include:
         - Host ME
         - Parent process ME

        :param proc: The process to get relations for.
        :param procs: A dictionary of all processes on the system.

        :returns: A list of relations, as :class:`cobe.UEID`s.
        """
        parents = []
        ppid = proc.ppid
        if ppid:
            if ppid in procs:
                pproc = procs[ppid]
                parents.append(self.get_ueid(pproc))
        else:
            parents.append(self.host_ueid)
        return parents

    def filtered_processes(self, attrs):
        """Filter processes based on attrs.

        Special case for 'pid' since this should be efficient.
        """
        if 'pid' in attrs and len(attrs) == 1:
            proc_containers = self.get_process_containers([attrs['pid']])
            try:
                proc = syskit.Process(attrs['pid'])
            except syskit.NoSuchProcessError:
                return
            entity = self.create_process_me(self.active_processes,
                                            proc, proc_containers)
            cpupc = self.get_cpu_percentage(proc)
            if cpupc:
                entity.attrs.set('cpu', cpupc,
                                 traits={'metric:gauge', 'unit:percent'})
            yield entity
        else:
            for proc in self.processes():
                try:
                    match = all([proc.attrs.get(name).value == value
                                 for (name, value) in attrs.items()])
                    if match:
                        yield proc
                except KeyError:
                    continue

    def processes(self):
        """Generator of Process MEs."""
        active = self.update_process_table(self.active_processes)
        create_me = functools.partial(self.create_process_me, active)
        processed_ueids = set()
        proc_containers = self.get_process_containers(active.keys())
        cpu_percentages = self.get_all_cpu_percentages()
        for proc in active.values():
            update = create_me(proc, proc_containers)
            try:
                update.attrs.set('cpu', cpu_percentages[proc.pid],
                                 traits={'metric:gauge', 'unit:percent'})
            except KeyError:
                pass
            processed_ueids.add(update.ueid)
            yield update
        self.active_processes = active

    def get_process_containers(self, pids):
        """Obtain the container IDs for all processes running in containers.

        Returns: A dict of the process PIDs to docker container IDs of format:

            {<process pid>: <container ID>, ...}.
        """
        if not self._docker_client:
            return {}
        containerids = {
            container['Id'] for container in self._docker_client.containers()}
        containers = {}
        for pid in pids:
            try:
                with open('/proc/{}/cgroup'.format(pid), 'r') as fp:
                    for containerid in containerids:
                        if containerid in fp.readline():
                            containers[pid] = containerid
            except FileNotFoundError:
                continue
        return containers

    @staticmethod
    def get_container_ueid(container_id):
        """Provide a container's ueid.

        :param str container_id: Container's 64-character docker id.

        :returns: A :class:`cobe.UEID` for the container.
        """
        update = entityd.EntityUpdate('Container')
        update.attrs.set(
            'id', 'docker://' + container_id, traits={'entity:id'})
        return update.ueid

    @staticmethod
    def update_process_table(procs):
        """Updates the process table, refreshing and adding processes.

        Returns a dict of active processes.

        :param procs: Dictionary mapping pid to syskit.Process

        """
        active = {}
        for pid in syskit.Process.enumerate():
            if pid in procs:
                proc = procs[pid]
                try:
                    proc.refresh()
                except syskit.NoSuchProcessError:
                    pass
                else:
                    active[pid] = proc
            else:
                try:
                    active[pid] = syskit.Process(pid)
                except (syskit.NoSuchProcessError, ProcessLookupError):
                    pass
        return active

    def get_cpu_percentage(self, proc):
        """Return CPU usage percentage since the last sample or process start.

        :param proc: syskit.Process instance.
        :returns float or None: the percentage value; None if no value

        """
        if not self.cpu_usage_sock:
            return None
        self.cpu_usage_sock.send_pyobj(proc.pid)
        if self.cpu_usage_sock.poll(timeout=1000, flags=zmq.POLLIN):
            return self.cpu_usage_sock.recv_pyobj()
        else:
            return None

    def get_all_cpu_percentages(self):
        """Return CPU usage percentage since the last sample or process start.

        :param proc: syskit.Process instance.
        :returns dict: All available percentage (possibly empty).
        """
        if not self.cpu_usage_sock:
            return {}
        self.cpu_usage_sock.send_pyobj(None)
        if self.cpu_usage_sock.poll(timeout=1000, flags=zmq.POLLIN):
            return self.cpu_usage_sock.recv_pyobj()
        else:
            return {}

    def create_process_me(self, proctable, proc, proc_containers):
        """Create a new Process ME structure for the process.

        Note that an entityd running in a container that shares the hosts's
        pid namespace will create entities of the same UEIDs as by an entityd
        running natively on the host. However, an entityd running in
        a container on the host that doesn't share the host's pid namespace
        (an approach not currently supported by Cobe) would create
        entities for the processes running in that container that would not
        be of the same UEIDs.

        Where a process is in a container, but doesn't have a ppid, then
        the UEID of the container of the process is added as a parent.

        :param proctable: Dict of pid -> syskit.Process instances for
           all processes on the host.
        :param proc: syskit.Process instance.
        :param proc_containers: Dict of all the PIDs of processes
           running in containers to docker container IDs.
        """
        update = entityd.EntityUpdate('Process')
        update.label = proc.name
        update.attrs.set('binary', proc.name)
        update.attrs.set('pid', proc.pid, traits={'entity:id'})
        update.attrs.set('starttime', proc.start_time.timestamp(),
                         traits={'entity:id', 'time:posix', 'unit:seconds'})
        update.attrs.set('ppid', proc.ppid)
        if proc.pid in proc_containers:
            update.attrs.set('containerid', proc_containers[proc.pid])
        update.attrs.set('host', str(self.host_ueid),
                         traits={'entity:id', 'entity:ueid'})
        update.attrs.set('cputime', float(proc.cputime),
                         traits={'metric:counter',
                                 'time:duration', 'unit:seconds'})
        update.attrs.set('utime', float(proc.utime),
                         traits={'metric:counter',
                                 'time:duration', 'unit:seconds'})
        update.attrs.set('stime', float(proc.stime),
                         traits={'metric:counter',
                                 'time:duration', 'unit:seconds'})
        update.attrs.set('vsz', proc.vsz,
                         traits={'metric:gauge', 'unit:bytes'})
        update.attrs.set('rss', proc.rss,
                         traits={'metric:gauge', 'unit:bytes'})
        update.attrs.set('uid', proc.ruid)
        update.attrs.set('euid', proc.euid)
        update.attrs.set('suid', proc.suid)
        try:
            update.attrs.set('username', proc.user)
        except syskit.AttrNotAvailableError:
            pass
        update.attrs.set('gid', proc.rgid)
        update.attrs.set('egid', proc.egid)
        update.attrs.set('sgid', proc.sgid)
        update.attrs.set('sessionid', proc.sid)
        update.attrs.set('command', proc.command)
        try:
            update.attrs.set('executable', proc.exe)
            update.attrs.set('args', proc.argv)
            update.attrs.set('argcount', proc.argc)
        except AttributeError:
            # A zombie process doesn't allow access to these attributes
            pass
        for parent in self.get_parents(proc, proctable):
            update.parents.add(parent)
        if proc.pid in proc_containers and proc.ppid not in proc_containers:
            update.parents.add(
                self.get_container_ueid(proc_containers[proc.pid]))
        return update
