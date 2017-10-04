"""Plugin to provide entities for docker containers."""

import logbook
from docker.errors import ImageNotFound

import entityd
from entityd.docker.client import DockerClient

log = logbook.Logger(__name__)


class DockerContainer:
    """Entity for a Docker Container."""
    name = "Docker:Container"

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs=None, include_ondemand=False):  # pylint: disable=unused-argument
        """Find Docker Container entities."""

        if name == self.name:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.generate_updates()

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Register the Process Monitored Entity."""
        config.addentity(self.name, 'entityd.docker.container.DockerContainer')

    @classmethod
    def get_ueid(cls, container_id):
        """Get a docker container ueid."""
        entity = entityd.EntityUpdate(cls.name)
        entity.attrs.set('id', container_id, traits={'entity:id'})
        return entity.ueid

    def generate_updates(self):
        """Generate entity update objects for each container."""
        if not DockerClient.client_available():
            return

        client = DockerClient.get_client()
        daemon_ueid = entityd.docker.get_ueid('DockerDaemon',
                                              client.info()['ID'])

        for container in client.containers.list(all=True):
            attrs = container.attrs

            update = entityd.EntityUpdate(self.name)
            update.label = container.name
            update.attrs.set('id', container.id, traits={'entity:id'})
            update.attrs.set('name', container.name)
            update.attrs.set('state:status', container.status)
            update.attrs.set('labels', container.labels)
            update.attrs.set('state:started-at', attrs['State']['StartedAt'],
                             traits={'chrono:rfc3339'})
            try:
                update.attrs.set('image:id', container.image.id)
                update.attrs.set('image:name', container.image.tags)
            except ImageNotFound:
                log.debug("Docker image ({}) not found",
                          container.attrs['Image'])
                update.attrs.set('image:id', None)
                update.attrs.set('image:name', None)
            else:
                update.parents.add(
                    entityd.docker.get_ueid('DockerImage', container.image.id))

            if container.status not in ["exited", "dead"]:
                update.attrs.set('state:exit-code', None)
                update.attrs.set('state:error', None)
                update.attrs.set('state:finished-at', None)
            else:
                update.attrs.set('state:exit-code', attrs['State']['ExitCode'])
                update.attrs.set('state:error', attrs['State']['Error'])
                update.attrs.set('state:finished-at',
                                 attrs['State']['FinishedAt'],
                                 traits={'chrono:rfc3339'})

            update.parents.add(daemon_ueid)
            for network in attrs['NetworkSettings']['Networks'].values():
                network_ueid = entityd.docker.get_ueid(
                    'DockerNetwork', network['NetworkID'])
                update.parents.add(network_ueid)

            yield update

