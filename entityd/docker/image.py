"""Plugin to provide entities for Docker images."""

import collections

import logbook
import docker.errors

import entityd
import entityd.docker
import entityd.docker.client
import entityd.docker.daemon


log = logbook.Logger(__name__)


class DockerImage:
    """Entity for a Docker Container."""

    _TYPES = {
        'Docker:Image': '_generate_images',
        'Docker:Image:Label': '_generate_labels',
    }

    def __init__(self):
        self._images = {}  # digest : image
        self._client_info = None

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs=None, include_ondemand=False):  # pylint: disable=unused-argument
        """Find Docker entities."""
        if name in self._TYPES:
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return getattr(self, self._TYPES[name])()

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Register the Process Monitored Entity."""
        for type_ in self._TYPES:
            config.addentity(
                type_, __name__ + '.' + self.__class__.__qualname__)

    @entityd.pm.hookimpl
    def entityd_collection_before(self, session):
        if not entityd.docker.client.DockerClient.client_available():
            return
        client = entityd.docker.client.DockerClient.get_client()
        self._client_info = client.info()
        for image in client.images.list(all=True):
            self._images[image.attrs['Id']] = image

    @entityd.pm.hookimpl
    def entityd_collection_after(self, session, updates):
        self._images.clear()
        self._client_info = None

    @classmethod
    def get_ueid(cls, digest):
        """Get the UEID for a Docker image.

        :param digest: Full image digest including the hash component.
        :type digest: str

        :returns: :class:`cobe.UEID` for the Docker image.
        """
        update = entityd.EntityUpdate('Docker:Image')
        update.attrs.set('digest', digest, {'entity:id'})
        return update.ueid

    def _image_label(self, update, image):
        update.label = image.attrs['Id'].split(':', 1)[-1][:12]
        for tag in sorted(image.attrs['RepoTags']):
            if not tag.endswith(':latest'):
                update.label = tag

    def _generate_images(self):
        for image in self._images.values():
            update = entityd.EntityUpdate('Docker:Image')
            self._image_label(update, image)
            self._image_parents(update, image)
            update.attrs.set('digest', image.attrs['Id'], {'entity:id'})
            update.attrs.set('created', image.attrs['Created'], {'time:rfc339'})
            update.attrs.set('size', image.attrs['Size'], {'unit:bytes'})
            update.attrs.set(
                'size:virtual', image.attrs['VirtualSize'], {'unit:bytes'})
            update.attrs.set('architecture', image.attrs['Architecture'])
            update.attrs.set('operating-system', image.attrs['Os'])
            update.attrs.set('docker:version', image.attrs['DockerVersion'])
            update.attrs.set('docker:driver', image.attrs['GraphDriver']['Name'])
            update.attrs.set('entry-point', image.attrs['Config']['Entrypoint'])
            update.attrs.set('command', image.attrs['Config']['Cmd'])
            update.attrs.set('dangling', not image.attrs['RepoTags'])
            yield update

    def _image_parents(self, update, image):
        digests = []
        if image.attrs['Parent']:
            digests.append(image.attrs['Parent'])
        for digest_repository in image.attrs['RepoDigests']:
            digests.append(digest_repository.rsplit('@', 1)[-1])
        for digest in digests:
            if digest in self._images:
                image_parent = self._images[image.attrs['Parent']]
                update.parents.add(self.get_ueid(image_parent.attrs['Id']))

    def _generate_labels(self):
        labels = collections.defaultdict(set)  # (key, value) : image digest
        for image in self._images.values():
            labels_image = image.attrs['Config']['Labels'] or {}
            for label_pair in labels_image.items():
                labels[label_pair].add(image.attrs['Id'])
        for (label_key, label_value), images in labels.items():
            update = entityd.EntityUpdate('Docker:Image:Label')
            update.label = "{0} = {1}".format(label_key, label_value)
            update.attrs.set('key', label_key, {'entity:id'})
            update.attrs.set('value', label_value, {'entity:id'})
            for image in images:
                update.children.add(self.get_ueid(image))
            yield update
