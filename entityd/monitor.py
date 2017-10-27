"""Monitor plugin

Responsible for collecting, monitoring and sending entities. Entities are
collected and their state is then monitored. If an entity is no longer
present then the entity will be configured as non existent.

The app main loop will call monitor.gather()
"""

import collections
import itertools

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

    @property
    def types(self):
        return set(self.config.entities) | set(self.last_batch)

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
            prefix = 'ueids/{}/'.format(metype)
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
            prefix = 'ueids/{}/'.format(metype)
            to_store = {}
            for ueid in entities:
                to_store[prefix + str(ueid)] = str(ueid)
            self.session.svc.kvstore.addmany(to_store)
        self.session.svc.kvstore.add('metypes', list(self.last_batch.keys()))
        entityd.health.die()

    @entityd.pm.hookimpl
    def entityd_emit_entities(self):
        """Wrapper for old-style entity update collection."""
        for metype in self.types:
            results = self.session.pluginmanager.hooks.entityd_find_entity(
                name=metype,
                attrs=None,
                include_ondemand=True,
                session=self.session,
            )
            for result in results:
                yield from result

    def collect_entities(self):
        """Collect and send all Monitored Entities."""
        log.info('Starting entity collection')
        self.session.pluginmanager.hooks.entityd_collection_before(
            session=self.session)
        updates = []
        this_batch = collections.defaultdict(set)
        for entity in itertools.chain.from_iterable(
                self.session.pluginmanager.hooks.entityd_emit_entities()):
            entityd.health.heartbeat()
            updates.append(entity)
            this_batch[entity.metype].add(entity.ueid)
        for metype, updates_type in this_batch.items():
            log.debug(
                'Collected {} {!r} entity updates', len(updates_type), metype)
        for metype in self.types:
            non_existent_ueids = self.last_batch[metype] - this_batch[metype]
            for ueid in non_existent_ueids:
                update = entityd.entityupdate.EntityUpdate(metype, str(ueid))
                update.set_not_exists()
                updates.append(update)
            if non_existent_ueids:
                log.debug('Generated {} {!r} entity deletions',
                          len(non_existent_ueids), metype)
            if not this_batch[metype]:
                del this_batch[metype]
        updates_merged = self._merge_updates(updates)
        if len(updates_merged) < len(updates):
            log.info('Merged {} entity updates; {} total',
                 len(updates) - len(updates_merged), len(updates))
        self._send_updates(updates_merged)
        self.last_batch = this_batch
        self.session.pluginmanager.hooks.entityd_collection_after(
            session=self.session, updates=tuple(updates_merged))

    def _merge_updates(self, updates):
        """Attempt to merge entity updates.

        For a given sequence of entity updates, if there are multiple
        updates for a UEID, those updates will be merged together. The
        resultant, merged entity update will be included in the new
        sequence of returned entity updates.

        If no merge is needed for a given UEID, the sole entity update
        for that UEID is included in the new sequence of entity updates
        unchanged.

        The order of the updates is preserved. If a merge occurs, the
        merged entity update will be placed at the position of the
        *first* update of that UEID.

        :param updates: Sequence of entity updates to merge.
        :type updates: Iterable of entityd.EntityUpdate

        :returns: List of entity updates.
        """
        updates_merged = []
        updates_ordered = []  # UEID
        updates_sequenced = {}  # UEID : updates
        for update in updates:
            if update.ueid not in updates_sequenced:
                updates_ordered.append(update.ueid)
                updates_sequenced[update.ueid] = []
            updates_sequenced[update.ueid].append(update)
        for ueid in updates_ordered:
            final, *changes = updates_sequenced[ueid]
            for change in changes:
                final = final.merge(change)
            updates_merged.append(final)
        return updates_merged

    def _send_updates(self, updates):
        """Enqueue entity updates to be sent."""
        for update in updates:
            self.session.pluginmanager.hooks.entityd_send_entity(
                session=self.session,
                entity=update,
            )
