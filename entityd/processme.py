"""Plugin providing the Process Monitored Entity."""

import base64
import functools
import time

import syskit

import entityd.pm


@entityd.pm.hookimpl
def entityd_plugin_registered(pluginmanager, name):
    """Called to register the plugin."""
    if name == 'entityd.processme':
        gen = ProcessEntity()
        pluginmanager.register(gen,
                               name='entityd.processme.ProcessEntity')


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

    @entityd.pm.hookimpl(after='entityd.kvstore')
    def entityd_sessionstart(self, session):
        """Load known ProcessME UEIDs."""
        self.session = session
        self.known_ueids = set(session.svc.kvstore.getmany(
            self.prefix).values())

    @entityd.pm.hookimpl(before='entityd.kvstore')
    def entityd_sessionfinish(self):
        """Called when the monitoring session ends."""
        self.session.svc.kvstore.deletemany(self.prefix)
        known_ueids = list(self.known_ueids)
        to_add = dict(zip([self.prefix.encode('ascii') + base64.b64encode(ueid)
                           for ueid in known_ueids],
                          known_ueids))
        self.session.svc.kvstore.addmany(to_add)

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs):
        """Return an iterator of "Process" Monitored Entities."""
        if name == 'Process':
            if attrs is not None:
                if 'pid' in attrs:
                    return self.process(attrs['pid'])
                raise LookupError('Attribute based filtering not supported '
                                  'for attrs {}'.format(attrs))
            return self.processes()

    @property
    def host_ueid(self):
        """Property to get the host ueid, used in a few places"""
        if not self._host_ueid:
            results = self.session.pluginmanager.hooks.entityd_find_entity(
                name='Host', attrs=None)
            if results:
                host_me = next(iter(results[0]))
                self._host_ueid = host_me.ueid
        return self._host_ueid

    def get_ueid(self, proc):
        """Get a cached ueid for this process if one exists, else generate one.

        :param proc: syskit.Process instance.
        """
        entity = entityd.EntityUpdate('Process')
        entity.attrs.set('pid', proc.pid, attrtype='id')
        entity.attrs.set('start_time', proc.start_time, attrtype='id')
        entity.attrs.set('host', self.host_ueid, attrtype='id')
        self.known_ueids.add(entity.ueid)
        return entity.ueid

    def forget_entity(self, update):
        """Remove the cached version of this Process Entity."""
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
        :returns: A list of relations, as wire-protocol dicts.

        """
        parents = []
        proc = procs[pid]
        ppid = proc.ppid
        if ppid and ppid in procs:
            pproc = procs[ppid]
            parents.append(self.get_ueid(pproc))
        if self.host_ueid:
            parents.append(self.host_ueid)
        return parents

    def process(self, pid):
        """Generate a single process ME for the process ID provided

        :param pid: Process ID to return
        """
        proctable = {}
        try:
            proc = syskit.Process(pid)
            proctable[pid] = proc
        except syskit.NoSuchProcessError:
            return

        if proc.ppid:
            try:
                pproc = syskit.Process(proc.ppid)
                proctable[proc.ppid] = pproc
            except syskit.NoSuchProcessError:
                pass

        yield self.create_process_me(proctable, proc)

    def processes(self):
        """Generator of Process MEs."""
        prev_processes = self.active_processes
        procs = self.process_table()
        create_me = functools.partial(self.create_process_me, procs)
        self.active_processes = {
            me.ueid: me
            for me in map(create_me, procs.values())
        }
        yield from self.active_processes.values()
        prev_ueids = set(prev_processes.keys()) | self.known_ueids
        active_ueids = set(self.active_processes.keys())
        deleted_ueids = prev_ueids - active_ueids
        for proc_ueid in deleted_ueids:
            try:
                update = prev_processes[proc_ueid]
            except KeyError:
                update = entityd.EntityUpdate('Process', ueid=proc_ueid)
            update.delete()
            self.forget_entity(update)
            yield update
        deleted_processes = (set(self._process_times.keys()) -
                             set(procs.values()))
        for proc in deleted_processes:
            del self._process_times[proc]

    @staticmethod
    def process_table():
        """Create a process table from syskit.Process.

        Sadly the syskit API does not make it easy to correctly create
        a process table.

        """
        procs = {}
        for pid in syskit.Process.enumerate():
            try:
                procs[pid] = syskit.Process(pid)
            except syskit.NoSuchProcessError:
                pass
        return procs

    def get_cpu_usage(self, proc):
        """Return cpu usage percentage since the last sample.

        """
        if proc not in self._process_times:
            last_cpu_time = 0
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
        update.attrs.set('binary', proc.name)
        update.attrs.set('pid', proc.pid, attrtype='id')
        update.attrs.set('starttime', proc.start_time.timestamp(),
                         attrtype='id')
        update.attrs.set('ppid', proc.ppid)
        update.attrs.set('host', self.host_ueid, attrtype='id')
        update.attrs.set('cputime', float(proc.cputime),
                         attrtype='perf:counter')
        update.attrs.set('cpu%', self.get_cpu_usage(proc),
                         attrtype='perf:gauge')
        update.attrs.set('vsz', proc.vsz, attrtype='perf:gauge')
        update.attrs.set('rss', proc.rss, attrtype='perf:gauge')
        # Note: there is a choice of (e)ffective, (s)aved or (r)eal uid
        update.attrs.set('uid', proc.ruid)
        update.attrs.set('euid', proc.euid)
        update.attrs.set('suid', proc.suid)

        update.attrs.set('username', proc.user)
        # Note: there is a choice of (e)ffective, (s)aved or (r)eal gid
        update.attrs.set('gid', proc.rgid)
        update.attrs.set('egid', proc.egid)
        update.attrs.set('sgid', proc.sid)
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
