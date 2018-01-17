"""Plugin to provide entities for docker containers."""

import logbook
from docker.errors import ImageNotFound

import entityd
import entityd.groups
from entityd.docker.client import DockerClient

log = logbook.Logger(__name__)


class DockerContainer:
    """Entity for a Docker Container."""

    name = "Docker:Container"

    @classmethod
    def get_ueid(cls, container_id):
        """Get a docker container ueid."""
        entity = entityd.EntityUpdate(cls.name)
        entity.attrs.set('id', container_id, traits={'entity:id'})
        return entity.ueid

    @entityd.pm.hookimpl
    def entityd_emit_entities(self):
        """Generate entity update objects for each container."""
        if not DockerClient.client_available():
            return

        client = DockerClient.get_client()
        daemon_id = client.info()['ID']
        daemon_ueid = entityd.docker.get_ueid('DockerDaemon', daemon_id)

        images = {}

        for container in DockerClient.all_containers():
            attrs = container.attrs
            update = entityd.EntityUpdate(self.name)
            update.label = container.name
            update.attrs.set('id', container.id, traits={'entity:id'})
            update.attrs.set('name', container.name)
            update.attrs.set('state:status', container.status)
            update.attrs.set('state:started-at', attrs['State']['StartedAt'],
                             traits={'chrono:rfc3339'})

            if attrs['Image'] not in images:
                try:
                    images[attrs['Image']] = container.image
                except ImageNotFound:
                    log.debug("Docker image ({}) not found",
                              container.attrs['Image'])

            image = images.get(attrs['Image'])

            if image:
                update.attrs.set('image:id', image.id)
                update.attrs.set('image:name', image.tags)
                update.parents.add(
                    entityd.docker.get_ueid('DockerImage', image.id))
            else:
                update.attrs.set('image:id', None)
                update.attrs.set('image:name', None)

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
            if attrs['NetworkSettings']['Networks']:
                for network in attrs['NetworkSettings']['Networks'].values():
                    network_ueid = entityd.docker.get_ueid(
                        'DockerNetwork', network['NetworkID'])
                    update.parents.add(network_ueid)

            yield update
            for group in entityd.groups.labels(container.labels):
                group.children.add(update)
                yield group
