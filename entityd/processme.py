"""Plugin providing the Process Monitored Entity."""

import functools
import time

import syskit

import entityd.pm


class ProcessEntity:
    """Plugin to generate Process MEs."""

    prefix = 'entityd.processme:'

    def __init__(self):
        self.active_processes = {}
        self.known_ueids = set()
        self.session = None
        self._host_ueid = None
        self._process_times = {}

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Register the Process Monitored Entity."""
        config.addentity('Process', 'entityd.processme.ProcessEntity')

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        """Store the session for later usage."""
        self.session = session

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
        self.known_ueids.add(entity.ueid)
        return entity.ueid

    def forget_entity(self, update):
        """Remove the cached version of this Process Entity.

        :param update: an entityd.EntityUpdate
        """
        try:
            self.known_ueids.remove(update.ueid)
        except KeyError:
            pass

    def get_parents(self, pid, procs):
        """Get relations for a process.

        Relations may include:
         - Host ME
         - Parent process ME

        :param pid: The process ID to get relations for.
        :param procs: A dictionary of all processes on the system.

        :returns: A list of relations, as :class:`cobe.UEID`s.
        """
        parents = []
        proc = procs[pid]
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
        active, deleted = self.update_process_table(self.active_processes)
        create_me = functools.partial(self.create_process_me, active)
        processed_ueids = set()

        for proc in deleted.values():
            del self._process_times[proc]

        # Active processes
        for proc in active.values():
            update = create_me(proc)
            processed_ueids.add(update.ueid)
            yield update

        self.active_processes = active

    @staticmethod
    def update_process_table(procs):
        """Updates the process table, refreshing and adding processes.

        Returns a tuple of two separate dicts of active and deleted processes.

        :param procs: Dictionary mapping pid to syskit.Process

        """
        active = {}
        deleted = {}
        for pid in syskit.Process.enumerate():
            if pid in procs:
                proc = procs[pid]
                try:
                    proc.refresh()
                except syskit.NoSuchProcessError:
                    deleted[pid] = proc
                else:
                    active[pid] = proc
            else:
                try:
                    active[pid] = syskit.Process(pid)
                except (syskit.NoSuchProcessError, ProcessLookupError):
                    pass
        return active, deleted

    def get_cpu_usage(self, proc):
        """Return CPU usage percentage since the last sample or process start.

        :param proc: syskit.Process instance.

        """
        if proc not in self._process_times:
            last_cpu_time = 0.0
            last_clock_time = proc.start_time.timestamp()
        else:
            old_proc = self._process_times[proc]
            last_cpu_time = float(old_proc.cputime)
            last_clock_time = old_proc.refreshed.timestamp()

        cpu_time = float(proc.cputime)
        clock_time = time.time()
        cpu_time_passed = cpu_time - last_cpu_time
        clock_time_passed = clock_time - last_clock_time
        percent_cpu_usage = (cpu_time_passed / clock_time_passed) * 100
        self._process_times[proc] = proc
        return percent_cpu_usage

    def create_process_me(self, proctable, proc):
        """Create a new Process ME structure for the process.

        :param proctable: Dict of pid -> syskit.Process instances for
           all processes on the host.
        :param proc: syskit.Process instance.

        """
        update = entityd.EntityUpdate('Process')
        update.label = proc.name
        update.attrs.set('binary', proc.name)
        update.attrs.set('pid', proc.pid, traits={'entity:id'})
        update.attrs.set('starttime', proc.start_time.timestamp(),
                         traits={'entity:id', 'time:posix', 'unit:seconds'})
        update.attrs.set('ppid', proc.ppid)
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
        update.attrs.set('cpu', self.get_cpu_usage(proc),
                         traits={'metric:gauge', 'unit:percent'})
        update.attrs.set('vsz', proc.vsz,
                         traits={'metric:gauge', 'unit:bytes'})
        update.attrs.set('rss', proc.rss,
                         traits={'metric:gauge', 'unit:bytes'})
        update.attrs.set('uid', proc.ruid)
        update.attrs.set('euid', proc.euid)
        update.attrs.set('suid', proc.suid)
        update.attrs.set('username', proc.user)
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
        for parent in self.get_parents(proc.pid, proctable):
            update.parents.add(parent)
        self.known_ueids.add(update.ueid)
        return update
