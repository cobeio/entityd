"""Plugin providing the Process Monitored Entity."""

import functools
import time
import uuid

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
        self.known_uuids = {}
        self.session = None
        self.host_uuid = None

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Register the Process Monitored Entity."""
        config.addentity('Process', 'entityd.processme.ProcessEntity')

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        """Load known ProcessME UUIDs.."""
        self.session = session
        self.known_uuids = session.svc.kvstore.getmany('entityd.processme:')

    @entityd.pm.hookimpl
    def entityd_sessionfinish(self):
        """Called when the monitoring session ends."""
        self.session.svc.kvstore.deletemany('entityd.processme:')
        self.session.svc.kvstore.addmany(self.known_uuids)

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs):
        """Return an iterator of "Process" Monitored Entities."""
        if name == 'Process':
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.processes()

    @staticmethod
    def _cache_key(pid, start_time):
        """Get a standard cache key for a process entity."""
        return 'entityd.processme:{}-{}'.format(pid, start_time)

    def get_uuid(self, proc):
        """Get a uuid for this process if one exists, else generate one.

        :param proc: syskit.Process instance.
        """
        key = self._cache_key(proc.pid, proc.start_time.timestamp())
        if key in self.known_uuids:
            return self.known_uuids[key]
        else:
            value = uuid.uuid4().hex
            self.known_uuids[key] = value
            return value

    def forget_entity(self, pid, start_time):
        """Remove the cached version of this Process Entity."""
        key = self._cache_key(pid, start_time)
        try:
            del self.known_uuids[key]
        except KeyError:
            pass

    def get_relations(self, pid, procs):
        """Get relations for a process.

        Relations may include:
         - Host ME
         - Parent process ME

        :param pid: The process ID to get relations for.
        :param procs: A dictionary of all processes on the system.
        :returns: A list of relations, as wire-protocol dicts.

        """
        relations = []
        proc = procs[pid]
        ppid = proc.ppid
        if ppid and ppid in procs:
            pproc = procs[ppid]
            relations.append({
                'uuid': self.get_uuid(pproc),
                'type': 'me:Process',
                'rel': 'parent'
            })
        if not self.host_uuid:
            results = self.session.pluginmanager.hooks.entityd_find_entity(
                name='Host', attrs=None)
            if results:
                host_me = next(iter(results[0]))
                self.host_uuid = host_me['uuid']
        if self.host_uuid:
            relations.append({
                'uuid': self.host_uuid,
                'type': 'me:Host',
                'rel': 'parent'
            })
        return relations

    def processes(self):
        """Generator of Process MEs."""
        prev_processes = self.active_processes
        procs = self.process_table()
        create_me = functools.partial(self.create_process_me, procs)
        self.active_processes = {
            me['uuid']: me
            for me in map(create_me, procs.values())
        }
        yield from self.active_processes.values()
        prev_uuids = set(prev_processes.keys())
        active_uuids = set(self.active_processes.keys())
        deleted_uuids = prev_uuids - active_uuids
        for proc_uuid in deleted_uuids:
            proc = prev_processes[proc_uuid]
            self.forget_entity(proc['attrs']['pid'],
                               proc['attrs']['starttime'])
            yield {
                'type': 'Process',
                'timestamp': time.time(),
                'uuid': proc_uuid,
                'delete': True
            }

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
        return {
            'type': 'Process',
            'timestamp': time.time(),
            'uuid': self.get_uuid(proc),
            'attrs': {
                'binary': proc.name,
                'pid': proc.pid,
                'starttime': proc.start_time.timestamp(),
            },
            'relations': self.get_relations(proc.pid, proctable)
        }
