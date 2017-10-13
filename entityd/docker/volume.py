"""This module contains the entity types for Docker Volumes."""

from entityd import EntityUpdate
from entityd.docker import BaseDocker, get_ueid
from entityd.docker.client import DockerClient


class DockerVolume(BaseDocker):
    """An entity for the docker volume."""
    name = "Docker:Volume"

    @classmethod
    def get_ueid(cls, daemon_id, docker_volume_name):
        """Create a ueid for a docker volume."""
        entity = EntityUpdate(cls.name)
        entity.attrs.set('daemon-id', daemon_id, traits={'entity:id'})
        entity.attrs.set('name', docker_volume_name, traits={'entity:id'})
        return entity.ueid

    def populate_volume_fields(self, volume, daemon_id):
        """Create EntityUpdate for a docker volume."""
        update = EntityUpdate(self.name)
        update.label = volume.attrs['Name']
        update.attrs.set('daemon-id', daemon_id, traits={'entity:id'})
        update.attrs.set('name', volume.attrs['Name'], traits={'entity:id'})
        update.attrs.set('labels', volume.attrs.get('Labels'))
        update.attrs.set('options', volume.attrs.get('Options'))
        update.attrs.set('driver', volume.attrs.get('Driver'))
        update.attrs.set('mount-point', volume.attrs.get('Mountpoint'))
        update.attrs.set('scope', volume.attrs.get('Scope'))

        return update

    def generate_updates(self):
        """Generates the entity updates for the docker volume."""
        if not DockerClient.client_available():
            return

        client = DockerClient.get_client()
        client_info = client.info()

        for volume in client.volumes.list():
            update = self.populate_volume_fields(volume, client_info['ID'])

            yield update


class DockerVolumeMount(BaseDocker):
    """An entity for the docker volume."""
    name = "Docker:Mount"

    @classmethod
    def get_ueid(cls, target, container_id):
        """Create a ueid for a docker volume."""
        entity = EntityUpdate(cls.name)
        entity.attrs.set('target', target, traits={'entity:id'})
        entity.attrs.set('container_id', container_id, traits={'entity:id'})
        return entity.ueid

    def populate_volume_mount_fields(self, volume, mount, container_id):
        """Create EntityUpdate for a docker volume."""
        update = EntityUpdate(self.name)
        update.label = volume.attrs['Name']
        target = mount['Destination']
        update.attrs.set('target', target, traits={'entity:id'})
        update.attrs.set('container_id', container_id, traits={'entity:id'})
        update.attrs.set('name', volume.attrs['Name'])
        update.attrs.set('volume:options', volume.attrs.get('Options'))
        update.attrs.set('volume:driver', volume.attrs.get('Driver'))
        update.attrs.set('volume:mount-point', volume.attrs.get('Mountpoint'))
        update.attrs.set('volume:scope', volume.attrs.get('Scope'))
        update.attrs.set('volume:mode', mount.get('Mode'))
        update.attrs.set('volume:read-write', mount.get('RW'))
        update.attrs.set('volume:source', mount.get('Source'))

        return update

    def generate_updates(self):
        """Generates the entity updates for the docker volume."""
        if not DockerClient.client_available():
            return

        client = DockerClient.get_client()
        daemon_id = DockerClient.info()['ID']
        volumes = {volume.name: volume for volume in client.volumes.list()}
        containers = {container.id: container for
                      container in client.containers.list(all=True)}

        for container in containers.values():
            for mount in container.attrs['Mounts']:
                mount_name = mount.get('Name')
                if mount_name in volumes:
                    volume = volumes[mount_name]
                    update = self.populate_volume_mount_fields(
                        volume, mount, container.id)
                    container_ueid = get_ueid('DockerContainer', container.id)
                    update.parents.add(container_ueid)

                    volume_ueid = get_ueid('DockerVolume',
                                           daemon_id, volume.attrs['Name'])
                    update.parents.add(volume_ueid)

                    yield update
