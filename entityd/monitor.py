"""Monitor plugin

Responsible for collecting, monitoring and sending entities. Entities are
collected and their state is then monitored. If an entity is no longer
present then the entity will be configured as non existent.

The app main loop will call monitor.gather()
"""

import base64
import collections

import cobe
import logbook

import entityd.health
import entityd.pm


log = logbook.Logger(__name__)


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
        try:
            last_types = set(session.svc.kvstore.get('metypes'))
        except KeyError:
            last_types = set()
        for metype in set(self.config.entities) | last_types:
            prefix = 'ueids:{}:'.format(metype)
            self.last_batch[metype] = set(
                cobe.UEID(ueid) for ueid in
                session.svc.kvstore.getmany(prefix).values()
            )
        entityd.health.heartbeat()

    @entityd.pm.hookimpl(before='entityd.kvstore')
    def entityd_sessionfinish(self):
        """Store out entities to kvstore."""
        self.session.svc.kvstore.deletemany('ueids:')
        for metype, entities in self.last_batch.items():
            prefix = 'ueids:{}:'.format(metype)
            to_store = {}
            for ueid in entities:
                key = prefix + base64.b64encode(str(ueid).encode()).decode()
                to_store[key] = str(ueid)
            self.session.svc.kvstore.addmany(to_store)
        self.session.svc.kvstore.add('metypes', list(self.last_batch.keys()))
        entityd.health.die()

    def collect_entities(self):
        """Collect and send all Monitored Entities."""
        log.info('Starting entity collection')
        types = set(self.config.entities) | set(self.last_batch)
        this_batch = collections.defaultdict(set)
        for metype in types:
            results = self.session.pluginmanager.hooks.entityd_find_entity(
                name=metype, attrs=None, include_ondemand=True)
            for result in results:
                for entity in result:
                    entityd.health.heartbeat()
                    self.session.pluginmanager.hooks.entityd_send_entity(
                        session=self.session, entity=entity)
                    this_batch[entity.metype].add(entity.ueid)
            if this_batch[metype]:
                log.debug('Sent {} {} entity updates.',
                          len(this_batch[metype]), metype)
        for metype in types:
            non_existent_ueids = self.last_batch[metype] - this_batch[metype]
            for ueid in non_existent_ueids:
                entityd.health.heartbeat()
                update = entityd.entityupdate.EntityUpdate(metype, str(ueid))
                update.set_not_exists()
                self.session.pluginmanager.hooks.entityd_send_entity(
                    session=self.session, entity=update)
            if non_existent_ueids:
                log.debug('Sent {} {} entity deletions.',
                          len(non_existent_ueids), metype)
            if not this_batch[metype]:
                del this_batch[metype]
        self.last_batch = this_batch
