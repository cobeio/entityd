"""Plugin providing the Process Monitored Entity."""

import functools

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

    def __init__(self):
        self.active_processes = {}
        self.known_ueids = {}
        self.session = None
        self._host_ueid = None

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Register the Process Monitored Entity."""
        config.addentity('Process', 'entityd.processme.ProcessEntity')

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        """Load known ProcessME UEIDs."""
        self.session = session
        self.known_ueids = session.svc.kvstore.getmany('entityd.processme:')

    @entityd.pm.hookimpl
    def entityd_sessionfinish(self):
        """Called when the monitoring session ends."""
        self.session.svc.kvstore.deletemany('entityd.processme:')
        self.session.svc.kvstore.addmany(self.known_ueids)

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

    @staticmethod
    def _cache_key(pid, start_time):
        """Get a standard cache key for a process entity."""
        return 'entityd.processme:{}-{}'.format(pid, start_time)

    def get_ueid(self, proc):
        """Get a cached ueid for this process if one exists, else generate one.

        :param proc: syskit.Process instance.
        """

        key = self._cache_key(proc.pid, proc.start_time.timestamp())
        if key in self.known_ueids:
            return self.known_ueids[key]
        else:
            entity = entityd.EntityUpdate('Process')
            entity.attrs.set('pid', proc.pid, attrtype='id')
            entity.attrs.set('start_time', proc.start_time, attrtype='id')
            entity.attrs.set('host', self.host_ueid, attrtype='id')
            value = entity.ueid
            self.known_ueids[key] = value
            return value

    def forget_entity(self, pid, start_time):
        """Remove the cached version of this Process Entity."""
        key = self._cache_key(pid, start_time)
        try:
            del self.known_ueids[key]
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
        prev_ueids = set(prev_processes.keys())
        active_ueids = set(self.active_processes.keys())
        deleted_ueids = prev_ueids - active_ueids
        for proc_ueid in deleted_ueids:
            update = prev_processes[proc_ueid]
            assert update.attrs.get('pid').value
            assert update.attrs.get('starttime').value
            self.forget_entity(update.attrs.get('pid').value,
                               update.attrs.get('starttime').value)
            update.delete()
            yield update

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
        for parent in self.get_parents(proc.pid, proctable):
            update.parents.add(parent)
        key = self._cache_key(proc.pid, proc.start_time.timestamp())
        self.known_ueids[key] = update.ueid
        return update
