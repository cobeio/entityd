"""Monitor plugin

Responsible for collecting, monitoring and sending entities. Entities are
collected and their state is then monitored. If an entity is no longer
present then the entity will be deleted.

The app main loop will call monitor.gather()
"""

import base64
import collections

import entityd.pm


@entityd.pm.hookimpl
def entityd_plugin_registered(pluginmanager, name):
    """Called to register the plugin."""
    if name == 'entityd.monitor':
        monitor = Monitor()
        pluginmanager.register(monitor,
                               name='entityd.monitor.Monitor')


class Monitor:
    """Plugin responsible for collecting, monitoring and sending entities."""

    def __init__(self):
        self.config = None
        self.session = None
        self.last_batch = collections.defaultdict(set)

    @entityd.pm.hookimpl(after='entityd.kvstore')
    def entityd_sessionstart(self, session):
        """Load entities from kvstore."""
        self.config = session.config
        self.session = session
        session.addservice('monitor', self)
        for metype in self.config.entities:
            prefix = 'ueids:{}:'.format(metype)
            self.last_batch[metype] = set(session.svc.kvstore.getmany(prefix)
                                          .values())

    @entityd.pm.hookimpl(before='entityd.kvstore')
    def entityd_sessionfinish(self):
        """Store out entities to kvstore."""
        for metype in self.config.entities:
            prefix = 'ueids:{}:'.format(metype)
            self.session.svc.kvstore.deletemany(prefix)
            to_store = {
                prefix + base64.b64encode(ueid).decode('ascii'): ueid
                for ueid in self.last_batch[metype]}
            self.session.svc.kvstore.addmany(to_store)

    def collect_entities(self):
        """Collect and send all Monitored Entities."""
        for metype in self.config.entities:
            results = self.session.pluginmanager.hooks.entityd_find_entity(
                name=metype, attrs=None)
            this_batch = set()
            for result in results:
                for entity in result:
                    self.session.pluginmanager.hooks.entityd_send_entity(
                        session=self.session, entity=entity)
                    this_batch.add(entity.ueid)

            deleted_ueids = self.last_batch[metype] - this_batch
            for ueid in deleted_ueids:
                update = entityd.entityupdate.EntityUpdate(metype, ueid)
                update.delete()
                self.session.pluginmanager.hooks.entityd_send_entity(
                    session=self.session, entity=update)
            self.last_batch[metype] = this_batch
