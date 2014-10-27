"""Plugin providing the Process Monitored Entity."""
import logging
import time
import uuid

import syskit

import entityd.pm


@entityd.pm.hookimpl
def entityd_plugin_registered(pluginmanager, name):
    if name == 'entityd.processme':
        gen = ProcessEntity()
        pluginmanager.register(gen,
                               name='entityd.processme.ProcessEntity')


class ProcessEntity:
    def __init__(self):
        self.active_processes = {}
        self.known_uuids = {}
        self.session = None

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        self.session = session

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Register the Host Monitored Entity."""
        config.addentity('Process', 'entityd.processme.ProcessEntity')

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs):
        """Return an iterator of "Host" Monitored Entities."""
        if name == 'Process':
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.processes()

    def get_uuid(self, pid, start_time):
        """Get a uuid for this process if one exists, else generate one

        :param pid: Process ID
        :param start_time: Start time of the process
        """
        key = 'entityd.processme:{}-{}'.format(pid, start_time)
        if key in self.known_uuids:
            logging.debug("Retrieved known process uuid from in memory store.")
            return self.known_uuids[key]

        value = self.session.pluginmanager.hooks.entityd_storage_get(key=key)
        if not value:
            logging.debug("No known uuid for process {}; creating one.".format(
                pid))
            value = uuid.uuid4().hex
            self.session.pluginmanager.hooks.entityd_storage_put(key=key,
                                                                 value=value)
        else:
            logging.debug("Retrieved known process uuid {} from sqlite store.")

        self.known_uuids[key] = value
        return value

    def get_relations(self, pid):
        """Get relations for ``pid``.

        Relations may include:
         - Host ME
         - Parent process ME
        """
        relations = []
        proc = syskit.Process(pid)
        ppid = proc.ppid
        if ppid:
            pproc = syskit.Process(ppid)
            relations.append({
                'uuid': self.get_uuid(ppid,
                                      pproc.start_time.timestamp()),
                'type': 'me:Process',
                'rel': 'parent'
            })
        elif ppid == 0:
            (host_me,), = self.session.pluginmanager.hooks.entityd_find_entity(
                name='Host', attrs=None)
            host_uuid = host_me['uuid']
            relations.append({
                'uuid': host_uuid,
                'type': 'me:Host',
                'rel': 'parent'
            })
        return relations

    def processes(self):
        """Generator of Host MEs"""
        processes = syskit.procs()

        # Filter out processes without a binary path attached
        processes = (p for p in processes if p[1])
        extra_info = (syskit.Process(pid) for (pid, command) in processes)

        active_processes = {
            self.get_uuid(pid, e.start_time.timestamp()):
                {
                    'type': 'Process',
                    'timestamp': time.time(),
                    'uuid': self.get_uuid(pid, e.start_time.timestamp()),
                    'attrs': {
                        'binary': path,
                        'pid': pid,
                        'starttime': e.start_time.timestamp(),
                    },
                    'relations': self.get_relations(pid)

                } for ((pid, path), e) in zip(processes, extra_info)
        }

        previously_active_uuids = set(self.active_processes.keys())
        active_uuids = set(active_processes.keys())
        deleted_uuids = previously_active_uuids - active_uuids
        logging.debug("{} deleted processes".format(len(deleted_uuids)))

        yield from active_processes.items()
        yield from (
            {
                'type': 'Process',
                'timestamp': time.time(),
                'uuid': uuid,
                'delete': True
            } for uuid in deleted_uuids
        )

        self.active_processes = active_processes